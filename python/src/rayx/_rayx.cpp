// rayx: minimal Python frontend over the HPX synthetic service lanes.
//
// Exposes (Python-facing wrappers add timing/lifecycle in __init__.py):
//   * _Engine(num_lanes, hpx_threads): starts HPX as a library, owns N
//     serialized rayhpx::ServiceLane instances, round-robins submissions.
//   * _Future: wraps one hpx::future<rayhpx::Result>; .result() blocks
//     (GIL released) and returns the C++-measured timing fields.
//   * hpx_smoke(): retained from the lifecycle spike, for debugging.
//
// Boundary measured by the Python driver over this module: hpx-python-frontend
// (Python -> in-process C++ over the SAME lane mechanism as the native exe;
// no process/IPC/serialization, but a pybind11/GIL crossing).
//
// Lifecycle: the HPX runtime is a process resource. _Engine enforces ONE
// active engine per process (start in ctor, stop in shutdown()); a second
// concurrent _Engine raises. Sequential engines after shutdown() are allowed
// but discouraged (HPX re-init, while observed to work, is not relied upon).
//
// HPX is linked as HPX::hpx only (no HPX::wrap_main): no main(), so the runtime
// is started via hpx::start / stopped via hpx::finalize + hpx::stop.

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>  // std::vector<double> <-> Python list/tuple (varied batch)

#include "service_lane.hpp"
#include "hpx_lane.hpp"  // rayhpx::HpxLane, the opt-in cooperative HPX-thread lane
#include "runtime_ops.hpp"  // rayx_runtime: Phase 1 registered-operation registry
#include "runtime_actor_ops.hpp"  // rayx_runtime: fixed local native actor registry (Slice B; header-only, no wiring yet)
#include "runtime_ops_hpx.hpp"  // rayx_runtime: HPX-side composed-op registry (fanout_sum)
#include "runtime_cancel.hpp"  // rayx_runtime::RuntimeCancelToken (Slice 2a)
#include "runtime_lane.hpp"  // rayx_runtime::RuntimeLane, the HPX-native FIFO lane (Slice 1)
#include "endpoint_seam.hpp"  // rayx_endpoint: experimental endpoint identity seam
#include "endpoint_transport.hpp"  // rayx_endpoint: A1 plain-IPC (AF_UNIX) transport

#include <hpx/hpx.hpp>
#include <hpx/hpx_start.hpp>

#include <atomic>
#include <condition_variable>  // RuntimeBridgeSlot drain gate
#include <memory>
#include <mutex>  // RuntimeBridgeSlot / ProcessHpxOwner critical sections
#include <optional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <unordered_map>  // RuntimeEngine::actors_ (actor_id -> ActorRecord)
#include <unordered_set>
#include <utility>  // std::pair (endpoint ping status/value)
#include <variant>  // std::visit over OpValue (int64|double) in RuntimeFuture::result
#include <vector>

#include <unistd.h>  // getpid() for the endpoint seam's cross-process discriminator

namespace py = pybind11;

namespace {

// ---- _Future ------------------------------------------------------------

// Move-only wrapper around one in-flight result future.
class EngineFuture {
public:
    explicit EngineFuture(
        hpx::future<rayhpx::Result> fut,
        std::shared_ptr<rayhpx::CancelToken> tok = nullptr)
        : fut_(std::move(fut)), tok_(std::move(tok)) {}

    EngineFuture(EngineFuture&&) = default;
    EngineFuture& operator=(EngineFuture&&) = default;
    EngineFuture(const EngineFuture&) = delete;
    EngineFuture& operator=(const EngineFuture&) = delete;

    // Block (GIL released) for the result; return C++-measured fields only.
    // The Python layer adds submit_ns/total_ms/queue_wait_ms (its own clock).
    py::dict result() {
        // Retiring consumes the future: fut_.get() moves the value out and
        // leaves the hpx::future invalid. Guard a second call with a clean
        // RayX-level error BEFORE releasing the GIL / calling fut_.get() again,
        // mirroring ready()'s guard (otherwise fut_.get() throws a raw HPX
        // no_state error).
        if (!fut_.valid()) {
            throw std::runtime_error(
                "Future is invalid (already retired via result()); cannot "
                "call result() again");
        }
        rayhpx::Result r;
        {
            py::gil_scoped_release release;
            r = fut_.get();
        }
        py::dict d;
        d["actor_id"] = r.actor_id;
        d["start_ns"] = r.start_ns;
        d["end_ns"] = r.end_ns;
        d["service_ms_observed"] = (r.end_ns - r.start_ns) / 1e6;
        d["status"] = r.status;
        // Lane-determined chunk accounting: how many active chunks actually ran.
        // == requested chunks on a normal finish, 0 on a queued-cancel, and in
        // [1, chunks-1] on a running (chunk-boundary) cancel. Carried on the C++
        // Result (the client cannot know where an early stop landed), unlike
        // chunks/chunk_delay_ms which the facade echoes from its own copy.
        d["chunks_completed"] = r.chunks_completed;
        if (r.error.empty()) {
            d["error"] = py::none();
        } else {
            d["error"] = r.error;
        }
        return d;
    }

    // Non-blocking: is the result ready to retire without blocking? Raises a
    // clear error if this future was already consumed by result() (a moved-from
    // hpx::future is invalid). Used as a cheap test hook and building block --
    // NOT as the basis for a Python busy-poll loop (use Engine.wait for that).
    bool ready() {
        if (!fut_.valid()) {
            throw std::runtime_error(
                "Future is invalid (already retired via result()); cannot "
                "query ready()");
        }
        return fut_.is_ready();
    }

    // Cancel a single submitted request. Returns true iff THIS call settles a
    // cancellation: either the request was still QUEUED (lane will skip service)
    // or it is RUNNING a chunked request with a chunk boundary still ahead (the
    // lane will stop at the next boundary -- "true" means guaranteed-to-stop, not
    // ready-now). Returns false otherwise: already started a single-chunk request,
    // already on its final chunk, already completed, or already cancelled. Raises
    // (clean RayX errors) if this future was already retired or is not cancelable.
    // A future is cancelable only if it was created with a CancelToken -- the
    // single-request submit path. Batch-submitted futures carry no token (no
    // per-future batch-cancel in this slice) and raise here rather than misreport
    // a misleading false. cancel() is NOT a retire: the cancelled future stays
    // valid and result() still returns its (cancelled) row exactly once.
    bool cancel() {
        if (!fut_.valid()) {
            throw std::runtime_error(
                "Future is invalid (already retired via result()); cannot "
                "cancel()");
        }
        if (!tok_) {
            throw std::runtime_error(
                "this Future is not cancelable; only single-request "
                "Engine.submit / SyntheticActor.remote futures support cancel "
                "(batch-submitted futures do not)");
        }
        bool did;
        {
            py::gil_scoped_release release;
            did = tok_->cancel();
        }
        return did;
    }

    // Non-blocking, non-consuming: has this future's cancellation been settled?
    // True once a queued cancel or a running stop-at-boundary is guaranteed (the
    // token's outcome-settled view), even before the lane has fulfilled the
    // cancelled row. Reads the token's phase only (never touches fut_), so it is
    // valid before AND after retire, and simply false for a non-cancelable
    // (token-less) future.
    bool cancelled() { return tok_ && tok_->is_cancelled(); }

    // --- borrow/return helpers for Engine::wait (no consumption) -------------
    // wait() moves the underlying future out (take), waits on a temp vector,
    // then moves it back (put), so the Python _Future object -- and its
    // Python-side submit_ns mapping -- is preserved.
    bool valid_now() const { return fut_.valid(); }
    hpx::future<rayhpx::Result> take() { return std::move(fut_); }
    void put(hpx::future<rayhpx::Result> f) { fut_ = std::move(f); }

private:
    hpx::future<rayhpx::Result> fut_;
    // Non-null only for cancelable (single-request) futures; self-contained, so
    // it stays safe to use even after the owning lane/engine is gone.
    std::shared_ptr<rayhpx::CancelToken> tok_;
};

// ---- rayx-local lane backend seam ---------------------------------------
//
// RayxLaneIface is the rayx Engine's OWN lane contract. It is deliberately
// SEPARATE from the native benchmark LaneIface in hpx_synthetic_baseline.cpp
// (which only covers the native FIFO-lane benchmark path and is left untouched):
// this one carries the full surface the Engine needs -- a cancel-token-aware
// submit, a bounded-admission try_submit, a non-cancelable submit_bulk, a
// non-consuming stats() snapshot, and the lane's stable actor_id. Two backends
// implement it through RayxLaneAdapter<Lane>: the default std::thread ServiceLane
// (the stable comparison anchor) and the opt-in cooperative HpxLane.
class RayxLaneIface {
public:
    // Neutral per-lane snapshot. ServiceLane::LaneStat and HpxLane::LaneStat are
    // distinct types; the adapter copies whichever it has into this one shape so
    // Engine::lane_stats() stays backend-agnostic. A plain value, never a
    // reference into a backend temporary.
    struct LaneStat {
        std::string actor_id;
        int queue_depth = 0;
        bool active = false;
    };

    virtual ~RayxLaneIface() = default;

    // out_tok: non-null requests a CancelToken (single-request cancelable path);
    // null leaves the request non-cancelable. Callers pass nullptr explicitly.
    virtual hpx::future<rayhpx::Result> submit(
        rayhpx::Request req,
        std::shared_ptr<rayhpx::CancelToken>* out_tok) = 0;
    // Bounded admission: std::nullopt iff the lane is at/over max_queue_depth.
    virtual std::optional<hpx::future<rayhpx::Result>> try_submit(
        rayhpx::Request req, int max_queue_depth,
        std::shared_ptr<rayhpx::CancelToken>* out_tok) = 0;
    // Non-cancelable bulk enqueue, one future per request in input order.
    virtual std::vector<hpx::future<rayhpx::Result>> submit_bulk(
        std::vector<rayhpx::Request> reqs) = 0;
    virtual LaneStat stats() = 0;
    virtual const std::string& actor_id() const = 0;
};

// RayxLaneAdapter<Lane> wires one backend lane to RayxLaneIface.
//
// ServiceLane (kHpxHop == false): every call forwards DIRECTLY to the lane on the
// calling (external Python) thread. This is behavior-equivalent to today's
// Engine-owns-ServiceLane code -- the interface/adapter add a layer of structure
// but no semantic change; ServiceLane uses std::mutex/std::thread, which are safe
// off an HPX thread.
//
// HpxLane (kHpxHop == true): HpxLane guards its queue with hpx::mutex /
// hpx::condition_variable_any and runs its worker as an hpx::thread, all of which
// must be touched only from an HPX thread. The Engine runs on the external Python
// thread, so the adapter hops EVERY lane-state operation -- construction (spawns
// the worker hpx::thread), destruction (locks hpx::mutex, joins the hpx::thread),
// submit, try_submit, submit_bulk, stats -- through hpx::run_as_hpx_thread, which
// runs the (cheap, enqueue-only) work on an HPX worker and blocks the caller for
// the result. actor_id() returns the lane's owned string and needs no hop.
// Cancellation is intentionally NOT routed here: CancelToken uses its own
// std::mutex + hpx::promise, so EngineFuture / token.cancel() stay on their
// existing no-hop path for BOTH backends.
template <class Lane>
class RayxLaneAdapter final : public RayxLaneIface {
    static constexpr bool kHpxHop = std::is_same_v<Lane, rayhpx::HpxLane>;

public:
    RayxLaneAdapter() {
        if constexpr (kHpxHop) {
            // HpxLane's ctor spawns an hpx::thread -> build it on an HPX thread.
            hpx::run_as_hpx_thread(
                [this]() { lane_ = std::make_unique<Lane>(); });
        } else {
            lane_ = std::make_unique<Lane>();
        }
    }

    ~RayxLaneAdapter() override {
        if constexpr (kHpxHop) {
            // ~HpxLane locks hpx::mutex and joins the worker hpx::thread -> run on
            // an HPX thread. HPX is still up here (Engine clears lanes BEFORE
            // hpx::finalize/stop, and ~Engine calls shutdown() before HPX stops).
            hpx::run_as_hpx_thread([this]() { lane_.reset(); });
        }
        // ServiceLane: the unique_ptr resets on this thread (joins its std::thread).
    }

    hpx::future<rayhpx::Result> submit(
        rayhpx::Request req,
        std::shared_ptr<rayhpx::CancelToken>* out_tok) override {
        if constexpr (kHpxHop) {
            return hpx::run_as_hpx_thread(
                [this, req = std::move(req), out_tok]() mutable {
                    return lane_->submit(std::move(req), out_tok);
                });
        } else {
            return lane_->submit(std::move(req), out_tok);
        }
    }

    std::optional<hpx::future<rayhpx::Result>> try_submit(
        rayhpx::Request req, int max_queue_depth,
        std::shared_ptr<rayhpx::CancelToken>* out_tok) override {
        if constexpr (kHpxHop) {
            return hpx::run_as_hpx_thread(
                [this, req = std::move(req), max_queue_depth,
                 out_tok]() mutable {
                    return lane_->try_submit(std::move(req), max_queue_depth,
                                             out_tok);
                });
        } else {
            return lane_->try_submit(std::move(req), max_queue_depth, out_tok);
        }
    }

    std::vector<hpx::future<rayhpx::Result>> submit_bulk(
        std::vector<rayhpx::Request> reqs) override {
        if constexpr (kHpxHop) {
            return hpx::run_as_hpx_thread(
                [this, reqs = std::move(reqs)]() mutable {
                    return lane_->submit_bulk(std::move(reqs));
                });
        } else {
            return lane_->submit_bulk(std::move(reqs));
        }
    }

    LaneStat stats() override {
        if constexpr (kHpxHop) {
            typename Lane::LaneStat s =
                hpx::run_as_hpx_thread([this]() { return lane_->stats(); });
            return LaneStat{s.actor_id, s.queue_depth, s.active};
        } else {
            typename Lane::LaneStat s = lane_->stats();
            return LaneStat{s.actor_id, s.queue_depth, s.active};
        }
    }

    const std::string& actor_id() const override { return lane_->actor_id(); }

private:
    std::unique_ptr<Lane> lane_;
};

// ---- _Engine ------------------------------------------------------------

std::vector<char> to_cstr(const std::string& s) {
    std::vector<char> v(s.begin(), s.end());
    v.push_back('\0');
    return v;
}

// ---- shared process-wide HPX runtime owner + bootstrap -------------------
//
// The HPX runtime is a process resource. A single ProcessHpxOwner (below) is the source
// of truth for "is HPX up and who holds it", replacing the old process_runtime_active()
// atomic. These two helpers are the low-level bring-up/teardown the owner calls; the
// hpx::start args and the finalize+stop order are unchanged.

void start_process_hpx(int hpx_threads) {
    std::vector<char> a0 = to_cstr("rayx");
    std::vector<char> a1 = to_cstr("--hpx:threads=" +
                                   std::to_string(hpx_threads));
    char* argv[] = {a0.data(), a1.data(), nullptr};
    int argc = 2;
    hpx::init_params params;
    py::gil_scoped_release release;
    if (!hpx::start(nullptr, argc, argv, params)) {
        throw std::runtime_error("hpx::start failed");
    }
}

void stop_process_hpx() {
    py::gil_scoped_release release;
    hpx::post([]() { hpx::finalize(); });
    hpx::stop();
}

// ProcessHpxOwner: single process-level HPX lifecycle + coexistence policy.
//
// Policy matrix:
//   * ENGINE   : exclusive  -- requires no Runtime and no Endpoint; blocks all others.
//   * RUNTIME  : singleton  -- one Runtime; may coexist with Endpoints; not with Engine.
//   * ENDPOINT : multiple   -- coexists with Runtime and other Endpoints; not with Engine.
//
// HPX lifecycle: ENGINE/RUNTIME are HPX-needing -- they start HPX on the first attach and
// stop it at the last detach (the only attachers that touch hpx::start/stop). ENDPOINT is
// HPX-FREE: it attaches for POLICY/bookkeeping only and never starts/stops HPX (the A1
// transport is plain native AF_UNIX IPC and the ping is computed inline). Because Engine
// and Runtime are mutually exclusive, at most one HPX-needing owner exists at a time, so
// its hpx_threads is authoritative and there is no thread-count negotiation.
//
// Synchronization: the CPython GIL serializes Python construction/teardown calls;
// `mu_` makes the native critical sections explicit (no free-threaded / no-GIL claim).
// stop runs on the calling (Python) thread only; NO HPX task may take `mu_` (nothing does
// -- Endpoint is HPX-free and Runtime fully drains its lanes BEFORE it detaches).
enum class HpxAttachKind { Engine, Runtime, Endpoint };

class ProcessHpxOwner {
public:
    static ProcessHpxOwner& instance() {
        static ProcessHpxOwner o;
        return o;
    }

    // Apply the policy matrix and, for ENGINE/RUNTIME, start HPX if not already up.
    // Throws std::runtime_error on a policy violation, leaving state unchanged.
    void attach(HpxAttachKind kind, int hpx_threads) {
        std::lock_guard<std::mutex> lk(mu_);
        switch (kind) {
            case HpxAttachKind::Engine:
                if (engine_ || runtime_ || endpoints_ > 0)
                    throw std::runtime_error(conflict_msg("Engine"));
                start_hpx_locked(hpx_threads);
                engine_ = true;
                break;
            case HpxAttachKind::Runtime:
                if (engine_ || runtime_)
                    throw std::runtime_error(conflict_msg("Runtime"));
                start_hpx_locked(hpx_threads);
                runtime_ = true;
                break;
            case HpxAttachKind::Endpoint:
                if (engine_)
                    throw std::runtime_error(conflict_msg("Endpoint"));
                ++endpoints_;  // HPX-free attacher: policy/bookkeeping only
                break;
        }
    }

    // Detach. ENGINE/RUNTIME drop the HPX refcount (stop HPX at the last one); ENDPOINT
    // just decrements. Guarded so a stray/duplicate detach cannot underflow or stop HPX
    // while an HPX-needing owner remains.
    void detach(HpxAttachKind kind) {
        std::lock_guard<std::mutex> lk(mu_);
        switch (kind) {
            case HpxAttachKind::Engine:
                if (engine_) { engine_ = false; stop_hpx_locked(); }
                break;
            case HpxAttachKind::Runtime:
                if (runtime_) { runtime_ = false; stop_hpx_locked(); }
                break;
            case HpxAttachKind::Endpoint:
                if (endpoints_ > 0) --endpoints_;
                break;
        }
    }

    bool hpx_active() {
        std::lock_guard<std::mutex> lk(mu_);
        return started_;
    }
    int endpoint_count() {
        std::lock_guard<std::mutex> lk(mu_);
        return endpoints_;
    }

private:
    ProcessHpxOwner() = default;

    void start_hpx_locked(int hpx_threads) {
        if (!started_) {
            start_process_hpx(hpx_threads);  // releases the GIL internally
            started_ = true;
            hpx_threads_ = hpx_threads;
        }
        ++hpx_refcount_;
    }
    void stop_hpx_locked() {
        if (hpx_refcount_ > 0 && --hpx_refcount_ == 0 && started_) {
            stop_process_hpx();  // releases the GIL; runs on the calling thread
            started_ = false;
        }
    }
    static std::string conflict_msg(const char* who) {
        return std::string(who) +
               "() cannot start: it conflicts with an already-active "
               "Engine/Runtime/Endpoint in this process (Engine is exclusive; Runtime is "
               "a singleton; Endpoint coexists with Runtime but not Engine). Shut down the "
               "conflicting owner first.";
    }

    std::mutex mu_;
    bool started_ = false;     // HPX up?
    int hpx_threads_ = 0;      // threads HPX was started with
    int hpx_refcount_ = 0;     // HPX-needing owners (Engine/Runtime)
    bool engine_ = false;
    bool runtime_ = false;
    int endpoints_ = 0;        // live Endpoints (HPX-free)
};

class Engine {
public:
    // max_queue_depth_per_lane: -1 (sentinel) = unbounded (default; preserves the
    // original behavior exactly). >= 1 = per-lane bounded admission: each lane
    // admits at most that many queued-but-not-started requests (see submit()).
    // The Python facade validates None/positive-int and passes -1 for None; the
    // <1 (non-sentinel) guard here is a defensive backstop.
    // lane_impl selects the backend behind every lane: "std" (default) = the
    // std::thread ServiceLane stable comparison anchor; "hpx" = the opt-in
    // cooperative HpxLane. Both implement the same RayxLaneIface contract; the
    // choice is invisible to the JSONL schema and visible only through the lane's
    // actor_id prefix (act-hpx- vs act-hpxl-). Validated up front so an unknown
    // value throws before any process resource (HPX runtime) is touched.
    Engine(int num_lanes, int hpx_threads, int max_queue_depth_per_lane,
           std::string lane_impl) {
        if (num_lanes < 1)
            throw std::invalid_argument("num_lanes must be >= 1");
        if (hpx_threads < 1)
            throw std::invalid_argument("hpx_threads must be >= 1");
        if (max_queue_depth_per_lane != -1 && max_queue_depth_per_lane < 1)
            throw std::invalid_argument(
                "max_queue_depth_per_lane must be -1 (unbounded) or >= 1");
        if (lane_impl != "std" && lane_impl != "hpx")
            throw std::invalid_argument(
                "lane_impl must be \"std\" or \"hpx\"");
        max_qd_per_lane_ = max_queue_depth_per_lane;

        // Engine is the EXCLUSIVE owner: attach starts HPX iff no Runtime/Endpoint holds
        // the process (throws otherwise). Detach on any partial-construction failure.
        ProcessHpxOwner::instance().attach(HpxAttachKind::Engine, hpx_threads);

        // Construct the lanes AFTER start_hpx: the HpxLane adapter ctor hops onto
        // an HPX thread to spawn the lane's worker, so the runtime must be up.
        // On a partial-construction failure, destroy whatever lanes already exist
        // (the adapter dtors join their workers, HpxLane via its own HPX hop --
        // same lanes-before-stop order as shutdown()), stop HPX, clear the guard,
        // and rethrow -- so a failed ctor leaves no live worker, no started
        // runtime, and no claimed guard. Mirrors RuntimeEngine's ctor cleanup.
        try {
            lanes_.reserve(static_cast<std::size_t>(num_lanes));
            for (int i = 0; i < num_lanes; ++i) {
                if (lane_impl == "hpx") {
                    lanes_.push_back(
                        std::make_unique<RayxLaneAdapter<rayhpx::HpxLane>>());
                } else {
                    lanes_.push_back(
                        std::make_unique<RayxLaneAdapter<rayhpx::ServiceLane>>());
                }
            }
        } catch (...) {
            try {
                lanes_.clear();  // adapter dtors join; HPX must still be up here
            } catch (...) {
                // best-effort cleanup; do not mask the original failure
            }
            ProcessHpxOwner::instance().detach(HpxAttachKind::Engine);  // stops HPX
            throw;
        }
        running_ = true;
    }

    ~Engine() {
        try {
            shutdown();
        } catch (...) {
            // best-effort; never throw from a destructor
        }
    }

    // Returns a Python _Future on admission, or Python None when a per-lane cap
    // is configured and the target lane is full (the facade raises QueueFullError
    // on None). py::object (rather than EngineFuture) is the return type precisely
    // so the rejected case can be a clean None without a sentinel future.
    py::object submit(double service_ms, const std::string& work_mode,
                      int chunks, double chunk_delay_ms) {
        if (!running_) throw std::runtime_error("Engine is shut down");
        rayhpx::Request req;
        req.service_ms_requested = service_ms;
        req.work_mode = work_mode;
        req.chunks = chunks;                  // chunked service (single-submit only)
        req.chunk_delay_ms = chunk_delay_ms;  // parked inter-chunk gap
        req.submit_ns = rayhpx::now_ns();
        RayxLaneIface& lane = *lanes_[rr_ % lanes_.size()];
        // rr_ advances on EVERY call -- admitted or rejected -- so call index i
        // always maps to lane (i % num_lanes) and one full lane never shifts the
        // rotation for later calls. (rr_ is Engine state, not a result-row field.)
        ++rr_;
        // Single-request submit is cancelable: request a CancelToken so
        // Engine::cancel can target this request while it is still queued.
        std::shared_ptr<rayhpx::CancelToken> tok;
        if (max_qd_per_lane_ >= 0) {
            // Bounded admission: check-and-push atomically under the lane mutex
            // (try_submit); std::nullopt means the lane was at/over the cap.
            std::optional<hpx::future<rayhpx::Result>> fut =
                lane.try_submit(std::move(req), max_qd_per_lane_, &tok);
            if (!fut) return py::none();  // rejected: no future, no row, no token
            return py::cast(EngineFuture(std::move(*fut), std::move(tok)),
                            py::return_value_policy::move);
        }
        auto fut = lane.submit(std::move(req), &tok);  // unbounded path unchanged
        return py::cast(EngineFuture(std::move(fut), std::move(tok)),
                        py::return_value_policy::move);
    }

    // Cancel a single submitted request (queued skip, or running stop at the
    // next chunk boundary). Mirrors the other running-engine guards (raises when
    // shut down); delegates the retired / not-cancelable / queued-vs-running /
    // boundary decision to the future + its token. Returns true iff this call
    // settles a cancellation.
    bool cancel(EngineFuture* ef) {
        if (!running_) throw std::runtime_error("Engine is shut down");
        return ef->cancel();
    }

    // Bulk submit: cross into C++ once, enqueue `count` requests using the same
    // round-robin lane routing as submit(), and return one _Future per request.
    // Returns a py::list (built here) because EngineFuture is move-only, so the
    // futures are cast out with return_value_policy::move rather than copied.
    py::list submit_batch(double service_ms, int count,
                          const std::string& work_mode) {
        if (!running_) throw std::runtime_error("Engine is shut down");
        if (count < 1) throw std::invalid_argument("count must be >= 1");
        std::vector<rayhpx::Request> reqs;
        reqs.reserve(static_cast<std::size_t>(count));
        for (int i = 0; i < count; ++i) {
            rayhpx::Request req;
            req.service_ms_requested = service_ms;
            req.work_mode = work_mode;
            req.submit_ns = rayhpx::now_ns();
            reqs.push_back(std::move(req));
        }
        return wrap_futures(enqueue_round_robin(std::move(reqs)));
    }

    // Varied bulk submit: same single-crossing, same round-robin lane routing as
    // submit_batch(), but each request carries its OWN service time from
    // service_ms_list (one request per element, in order). This is the C++ path
    // behind the facade's list form (engine.submit_batch(service_ms=[...])); it
    // is NOT a Python loop over submit(), so the bulk property holds (one
    // Python->C++ crossing; the facade stamps all returned futures with one
    // shared Python submit_ns). The Python layer validates the list
    // (non-empty, finite, > 0) before this is called; the empty guard here is a
    // defensive backstop. work_mode is the single shared mode for the batch.
    py::list submit_batch_varied(const std::vector<double>& service_ms_list,
                                 const std::string& work_mode) {
        if (!running_) throw std::runtime_error("Engine is shut down");
        if (service_ms_list.empty())
            throw std::invalid_argument("service_ms list must be non-empty");
        std::vector<rayhpx::Request> reqs;
        reqs.reserve(service_ms_list.size());
        for (double svc : service_ms_list) {
            rayhpx::Request req;
            req.service_ms_requested = svc;
            req.work_mode = work_mode;
            req.submit_ns = rayhpx::now_ns();
            reqs.push_back(std::move(req));
        }
        return wrap_futures(enqueue_round_robin(std::move(reqs)));
    }

    // INTERNAL DIAGNOSTIC -- not public API, not documented, not on the
    // Engine/SyntheticActor Python facade (reached only as
    // engine._engine._submit_batch_cost_probe(...)). Attributes the per-request
    // cost of the submit_batch path so we can decide whether a lane-level bulk
    // enqueue is worth building, WITHOUT changing submit_batch itself or the
    // result-row/JSONL schema. service_ms is forced to 0 (no-op): we are timing
    // control overhead, not service. Three separately-timed phases over `count`
    // requests, GIL held throughout phases 1-2 to match the real path:
    //   1. enqueue  -- the lane enqueue path ONLY (enqueue_round_robin: per-lane
    //      bulk or one-by-one lock+push+notify), futures collected into a
    //      std::vector (NOT wrapped). Requests are BUILT BEFORE the timer, so
    //      enqueue_ns isolates the enqueue strategy and excludes BOTH request
    //      construction and the Python list-append (the latter is phase 2).
    //   2. wrap     -- move each future into an EngineFuture and py::cast it into
    //      a py::list, the EXACT pybind/list-append the real submit_batch does.
    //   3. drain    -- result() every future so nothing dangles before return
    //      (service_ms=0 -> immediate); keeps engine shutdown clean.
    // Returns a small dict of ns timings. Advances rr_ by `count` (harmless;
    // intended for a throwaway engine). service_lane.hpp is untouched.
    py::dict submit_batch_cost_probe(int count, const std::string& work_mode) {
        if (!running_) throw std::runtime_error("Engine is shut down");
        if (count < 1) throw std::invalid_argument("count must be >= 1");
        const std::size_t L = lanes_.size();

        // Phase 1: enqueue only (no pybind wrapping), through the SAME
        // enqueue_round_robin path the real submit_batch uses -- so flipping the
        // bulk/single mode (_set_bulk_enqueue) A/Bs exactly what ships. Request
        // construction is done BEFORE t0 so enqueue_ns isolates the enqueue
        // strategy (single per-request lock+notify vs one bulk lock+notify per
        // lane), not request building.
        std::vector<rayhpx::Request> reqs;
        reqs.reserve(static_cast<std::size_t>(count));
        for (int i = 0; i < count; ++i) {
            rayhpx::Request req;
            req.service_ms_requested = 0.0;  // no-op: timing control overhead only
            req.work_mode = work_mode;
            req.submit_ns = rayhpx::now_ns();
            reqs.push_back(std::move(req));
        }
        const std::int64_t t0 = rayhpx::now_ns();
        std::vector<hpx::future<rayhpx::Result>> futs =
            enqueue_round_robin(std::move(reqs));
        const std::int64_t t1 = rayhpx::now_ns();

        // Phase 2: wrap each future into a Python EngineFuture in a py::list,
        // exactly as submit_batch does (the pybind object + list-append cost).
        const std::int64_t t2 = rayhpx::now_ns();
        py::list wrapped;
        for (auto& f : futs) {
            EngineFuture ef(std::move(f));
            wrapped.append(py::cast(std::move(ef), py::return_value_policy::move));
        }
        const std::int64_t t3 = rayhpx::now_ns();

        // Phase 3: drain/settle every request (service_ms=0 -> immediate) so no
        // promise dangles across a later shutdown. result() retires each future.
        const std::int64_t t4 = rayhpx::now_ns();
        for (py::handle h : wrapped) {
            h.cast<EngineFuture*>()->result();
        }
        const std::int64_t t5 = rayhpx::now_ns();

        py::dict d;
        d["requests"] = count;
        d["lanes"] = static_cast<int>(L);
        d["enqueue_ns"] = t1 - t0;  // lane enqueue only (enqueue_round_robin); request build is BEFORE t0
        d["pybind_wrap_ns"] = t3 - t2;
        d["drain_ns"] = t5 - t4;
        d["total_ns"] = (t1 - t0) + (t3 - t2);  // producer-side submit_batch cost
        return d;
    }

    // As-completed wait. Block (GIL released) until at least num_returns of the
    // given _Future objects are ready, then return the indices (into the input
    // list) of ALL currently-ready futures. The caller retires the ones it
    // wants and keeps the rest in flight -- this is the primitive behind the
    // batch_wait retire loop and mirrors ray.wait(num_returns=k).
    //
    // Why hpx::wait_some: it is the one HPX combinator that spans both
    // num_returns==1 (wait_any) and num_returns>1 uniformly. hpx::wait_any only
    // covers k==1; hpx::when_any returns a future<when_any_result> (a heavier
    // composer that allocates a continuation and moves the inputs into its
    // result) -- the wrong shape for this simple blocking partition.
    //
    // It does NOT consume any future. hpx::wait_some acquires each future's
    // *shared state* and guarantees "all input futures are still valid after
    // wait_some returns" (HPX 1.11 docs), so it never reads/invalidates a value.
    // We still move the underlying hpx::futures into a temp vector and back into
    // the SAME _Future objects only because hpx::future is move-only and we use
    // the std::vector overload; the futures live in separate pybind objects, not
    // contiguously. (An iterator overload could wait over references in place,
    // but the explicit move-out + RAII restore is clearer and avoids a custom
    // future-iterator adaptor.) Python keeps ownership; each Future's Python-side
    // submit_ns is preserved.
    //
    // Threading: wait_some here blocks the *external Python caller OS thread*,
    // not an HPX worker thread (the runtime was started via hpx::start on its
    // own threads), so blocking it does not starve the HPX scheduler. The GIL is
    // released around the blocking wait so other Python threads can run; it is
    // never a busy-poll under the GIL.
    py::list wait(py::list futures, int num_returns) {
        if (!running_) throw std::runtime_error("Engine is shut down");
        const std::size_t n = futures.size();
        if (n == 0) throw std::invalid_argument("wait() got an empty futures list");
        if (num_returns < 1)
            throw std::invalid_argument("num_returns must be >= 1");
        if (static_cast<std::size_t>(num_returns) > n)
            throw std::invalid_argument("num_returns must be <= len(futures)");

        // Borrow the C++ wrappers (Python retains the objects). A non-_Future
        // entry raises TypeError via pybind's cast; an already-retired future
        // raises a clear error. A duplicate (the same underlying _Future seen
        // twice) is rejected here, BEFORE any take() below -- otherwise we would
        // move the same hpx::future out twice and corrupt it.
        std::vector<EngineFuture*> efs;
        efs.reserve(n);
        std::unordered_set<EngineFuture*> seen;
        seen.reserve(n);
        for (py::handle h : futures) {
            EngineFuture* ef = h.cast<EngineFuture*>();
            if (!ef->valid_now()) {
                throw std::runtime_error(
                    "wait() received a Future already retired via result()");
            }
            if (!seen.insert(ef).second) {
                throw std::invalid_argument(
                    "wait() received the same Future more than once; each "
                    "Future may appear at most once");
            }
            efs.push_back(ef);
        }

        // Move each underlying future out into `tmp`, guarded by a RAII Restore
        // that moves them back into the SAME _Future objects on EVERY exit path
        // (normal return or an exception out of wait_some). The guard is
        // installed BEFORE the take loop and restores exactly as many as were
        // taken (`tmp.size()`), so even a partial take is unwound cleanly and a
        // Python _Future is never left moved-from. (In practice take() and the
        // reserved push_back are noexcept, so the take loop itself does not
        // throw; the guard's real job is the blocking wait_some below.)
        std::vector<hpx::future<rayhpx::Result>> tmp;
        tmp.reserve(n);
        struct Restore {
            std::vector<EngineFuture*>& efs;
            std::vector<hpx::future<rayhpx::Result>>& tmp;
            ~Restore() {
                for (std::size_t i = 0; i < tmp.size(); ++i)
                    efs[i]->put(std::move(tmp[i]));
            }
        } restore{efs, tmp};
        for (EngineFuture* ef : efs) tmp.push_back(ef->take());

        {
            // Release the GIL ONLY around the blocking HPX wait. No Python
            // objects are touched inside this scope (tmp holds C++ futures); the
            // ready-list construction below runs with the GIL reacquired.
            py::gil_scoped_release release;
            hpx::wait_some(static_cast<std::size_t>(num_returns), tmp);
        }

        py::list ready;
        for (std::size_t i = 0; i < tmp.size(); ++i) {
            if (tmp[i].is_ready()) ready.append(static_cast<int>(i));
        }
        return ready;  // ~Restore() moves the futures back into the _Futures
    }

    int num_lanes() const { return static_cast<int>(lanes_.size()); }

    // Observability snapshot (debugging only): one dict per lane, in stable lane
    // order, with the lane's queued-but-not-started count and whether it is
    // currently inside a request's service lifecycle. Briefly takes each lane's
    // mutex (RayxLaneIface::stats, hopping onto an HPX thread for the hpx
    // backend); NON-consuming -- it touches no future and
    // changes no submit/service/cancel semantics. Snapshot only: values can change
    // the instant after this returns. Raises if the engine is shut down (lanes are
    // destroyed), consistent with the other coordination/new-work APIs. Not Ray
    // scheduler state, not placement control, not part of the JSONL schema.
    py::list lane_stats() {
        if (!running_) throw std::runtime_error("Engine is shut down");
        py::list out;
        for (auto& lane : lanes_) {
            RayxLaneIface::LaneStat s = lane->stats();
            py::dict d;
            d["actor_id"] = s.actor_id;
            d["queue_depth"] = s.queue_depth;
            d["active"] = s.active;
            out.append(std::move(d));
        }
        return out;
    }

    // INTERNAL A/B toggle (not public API): select the batch enqueue strategy.
    // true (default) = per-lane bulk enqueue (one lock+notify per lane); false =
    // original one-by-one enqueue (one lock+notify per request). Affects only the
    // batch paths (submit_batch / submit_batch_varied) and the cost probe; does
    // not change observable behavior (order, actor_id, schema), only how the
    // requests are pushed onto the lanes. Exposed as _set_bulk_enqueue for local
    // measurement; never wired into the Python facade.
    void set_bulk_enqueue(bool on) { use_bulk_enqueue_ = on; }

    void shutdown() {
        if (!running_) return;
        running_ = false;
        // Join lane threads first (drains queued requests), then detach (stops HPX as the
        // last HPX-needing owner).
        lanes_.clear();
        ProcessHpxOwner::instance().detach(HpxAttachKind::Engine);
    }

private:
    // Wrap a vector of result futures (in order) into a py::list of _Future
    // objects -- the move-only EngineFuture is cast out with move policy, exactly
    // as the old per-request loop did. Shared by submit_batch / submit_batch_varied.
    py::list wrap_futures(std::vector<hpx::future<rayhpx::Result>> futs) {
        py::list out;
        for (auto& f : futs) {
            EngineFuture ef(std::move(f));
            out.append(py::cast(std::move(ef), py::return_value_policy::move));
        }
        return out;
    }

    // Enqueue a request group across the lanes with the SAME round-robin mapping
    // the per-request path uses -- lane_i = (rr_start + i) % num_lanes -- and
    // return one future per request IN INPUT ORDER. Advances rr_ by the group
    // size. Batch requests are non-cancelable (no CancelToken) and unchunked, so
    // neither strategy attaches a token. Two strategies, A/B-selectable via
    // use_bulk_enqueue_:
    //   * bulk (default): group requests per lane (preserving input order within
    //     a lane), call ServiceLane::submit_bulk once per lane (one lock + one
    //     notify per lane), then scatter each lane's futures back to their
    //     original input indices.
    //   * single: ServiceLane::submit per request (one lock + notify each) -- the
    //     original behavior, kept for measurement.
    std::vector<hpx::future<rayhpx::Result>> enqueue_round_robin(
            std::vector<rayhpx::Request> reqs) {
        const std::size_t L = lanes_.size();
        const std::size_t n = reqs.size();
        const std::size_t rr_start = rr_;
        rr_ += n;
        std::vector<hpx::future<rayhpx::Result>> ordered(n);
        if (use_bulk_enqueue_) {
            std::vector<std::vector<rayhpx::Request>> per_lane(L);
            std::vector<std::vector<std::size_t>> per_lane_idx(L);
            for (std::size_t i = 0; i < n; ++i) {
                const std::size_t lane = (rr_start + i) % L;
                per_lane[lane].push_back(std::move(reqs[i]));
                per_lane_idx[lane].push_back(i);
            }
            for (std::size_t l = 0; l < L; ++l) {
                if (per_lane[l].empty()) continue;
                std::vector<hpx::future<rayhpx::Result>> futs =
                    lanes_[l]->submit_bulk(std::move(per_lane[l]));
                for (std::size_t k = 0; k < futs.size(); ++k)
                    ordered[per_lane_idx[l][k]] = std::move(futs[k]);
            }
        } else {
            for (std::size_t i = 0; i < n; ++i) {
                const std::size_t lane = (rr_start + i) % L;
                ordered[i] = lanes_[lane]->submit(std::move(reqs[i]), nullptr);
            }
        }
        return ordered;
    }

    std::vector<std::unique_ptr<RayxLaneIface>> lanes_;
    std::size_t rr_ = 0;
    // Per-lane bounded-admission cap: -1 = unbounded (default); >= 1 = max
    // queued-but-not-started requests admitted per lane (see submit()).
    int max_qd_per_lane_ = -1;
    bool running_ = false;
    // Batch enqueue strategy (A/B; see set_bulk_enqueue). Default = bulk.
    bool use_bulk_enqueue_ = true;
};

// ---- rayx.runtime registered-operation runtime --------------------------
//
// EXPERIMENTAL, additive, and fully SEPARATE from the harness above. The runtime
// layer dispatches a fixed native operation registry (rayx_runtime::registry)
// over N HPX-native FIFO RuntimeLanes, returning a user value PLUS the core
// measurement-row fields as TWO separate things (the Python layer puts the value
// on OperationResult.value and the row on OperationResult.row). It provides
// round-robin lanes + num_lanes/lane_stats, cooperative queued/running
// cancellation, bounded per-lane admission, and non-consuming wait/as_completed.
//
// It is NOT Ray and NOT Ray-compatible: no object store, no ObjectRef, no HPX
// actions/components, no distributed locality, and no arbitrary Python execution.

// Move-only wrapper around one in-flight runtime result future. Consume-once,
// mirroring EngineFuture's valid()-guard so a second result() raises a clean
// RayX-level error instead of a raw HPX no_state.
class RuntimeFuture {
public:
    RuntimeFuture(hpx::future<rayx_runtime::RuntimeResult> fut,
                  std::shared_ptr<rayx_runtime::RuntimeCancelToken> tok)
        : fut_(std::move(fut)), tok_(std::move(tok)) {}

    RuntimeFuture(RuntimeFuture&&) = default;
    RuntimeFuture& operator=(RuntimeFuture&&) = default;
    RuntimeFuture(const RuntimeFuture&) = delete;
    RuntimeFuture& operator=(const RuntimeFuture&) = delete;

    // Block (GIL released) for the result; return the C++-measured fields plus
    // the operation value. The Python layer splits these into OperationResult
    // (value vs row) and adds submit_ns/total_ms/queue_wait_ms (its own clock).
    py::dict result() {
        if (!fut_.valid()) {
            throw std::runtime_error(
                "RuntimeFuture is invalid (already retired via result()); "
                "cannot call result() again");
        }
        rayx_runtime::RuntimeResult r;
        {
            py::gil_scoped_release release;
            r = fut_.get();
        }
        py::dict d;
        d["actor_id"] = r.actor_id;
        d["start_ns"] = r.start_ns;
        d["end_ns"] = r.end_ns;
        d["service_ms_observed"] = (r.end_ns - r.start_ns) / 1e6;
        d["status"] = r.status;
        if (r.error.empty()) {
            d["error"] = py::none();
        } else {
            d["error"] = r.error;
        }
        // Value channel (kept OUT of the row by the Python layer): has_value is
        // false for a failed/cancelled operation -> .value raises. The native value
        // is an OpValue variant (int64|double); convert it to a Python object HERE,
        // on the Python thread with the GIL held (this method releases the GIL only
        // around fut_.get() above), via an explicit std::visit -- int64 -> Python int,
        // double -> Python float. No Python object is ever built on an HPX worker.
        d["has_value"] = r.has_value;
        if (r.has_value) {
            d["value"] = std::visit(
                [](const auto& v) -> py::object { return py::cast(v); }, r.value);
        } else {
            d["value"] = py::none();
        }
        return d;
    }

    bool ready() {
        if (!fut_.valid()) {
            throw std::runtime_error(
                "RuntimeFuture is invalid (already retired via result()); "
                "cannot query ready()");
        }
        return fut_.is_ready();
    }

    // Cancel this operation. Runs DIRECTLY on the external Python thread (the token
    // is a self-contained std::mutex state machine + a copy of the promise -- NO
    // hpx::run_as_hpx_thread hop). Returns true iff THIS call settles a
    // cancellation: the op was still QUEUED (lane will skip it) or RUNNING a
    // checkpointed op with a boundary still ahead (it stops at the next checkpoint).
    // Returns false otherwise (already completed/cancelled, or running a
    // checkpoint_count == 1 op). Safe after retire (terminal phase -> false).
    bool cancel() { return tok_ ? tok_->cancel() : false; }

    // Non-consuming: true once cancellation is guaranteed (queued cancel, or a
    // requested running stop), valid before AND after result().
    bool cancelled() { return tok_ ? tok_->is_cancelled() : false; }

    // Internal helpers for RuntimeEngine::wait (NOT pybind-exposed), mirroring
    // EngineFuture::valid_now/take/put. wait() must move the move-only hpx::future
    // out into a temp vector for the hpx::wait_some std::vector overload and move it
    // back into the SAME RuntimeFuture afterwards. Only fut_ moves; the cancel token
    // tok_ is untouched, so cancel semantics survive a wait.
    bool valid_now() const { return fut_.valid(); }
    hpx::future<rayx_runtime::RuntimeResult> take() { return std::move(fut_); }
    void put(hpx::future<rayx_runtime::RuntimeResult> f) { fut_ = std::move(f); }

private:
    hpx::future<rayx_runtime::RuntimeResult> fut_;
    std::shared_ptr<rayx_runtime::RuntimeCancelToken> tok_;
};

// Build the service-slot closure for one operation: runs the op body, times it on
// the same monotonic clock the row uses, and maps the outcome (value/failure) into
// a RuntimeResult stamped with the SERVING LANE's actor_id. Lives here (not in
// RuntimeLane) because it needs the registry's OpFn + rayhpx::now_ns + the
// value/failure mapping; the lane just runs the closure HPX-natively. start_ns is
// read INSIDE the closure (when the lane actually runs it), so the value reflects
// service-slot occupancy, not enqueue/async-scheduling latency.
inline rayx_runtime::RuntimeLane::OpTask make_op_task(
        rayx_runtime::OpFn fn, rayx_runtime::OpArgs args,
        std::string actor) {
    return [fn = std::move(fn), args = std::move(args), actor = std::move(actor)](
               const rayx_runtime::StopCheckpoint& stop)
               -> rayx_runtime::RuntimeResult {
        rayx_runtime::RuntimeResult r;
        r.actor_id = actor;
        r.start_ns = rayhpx::now_ns();
        try {
            rayx_runtime::OpOutcome o = fn(args, stop);
            r.value = o.value;
            r.has_value = o.has_value;
            r.status = o.status;
            r.error = o.error;
        } catch (const std::exception& e) {
            // Operation exception -> failed result + error (P8).
            r.status = "failed";
            r.error = e.what();
            r.has_value = false;
        } catch (...) {
            // Defensive backstop: a non-std::exception throw still maps to a
            // failed result rather than escaping the task. The built-ins only
            // throw std::runtime_error, so no current op reaches this path.
            r.status = "failed";
            r.error = "operation failed with a non-std::exception";
            r.has_value = false;
        }
        r.end_ns = rayhpx::now_ns();
        return r;
    };
}

// Marshal a Python arg sequence into the typed OpArgs value channel for the ACTOR
// path. C1b MIRRORS submit_operation's inline marshalling deliberately (it is NOT a
// shared refactor yet -- the op path stays byte-for-byte unchanged); a consolidation
// into one helper is a later slice. Same rules: bool rejected BEFORE int (bool is a
// Python int subclass); int -> int64; float -> double; anything else -> a clear
// error. Runs on the Python thread (GIL held); no Python object is built on an HPX
// worker. `what` is the already-formatted context noun so the message reads "actor
// 'counter' init argument N ..." / "method 'add' argument N ..." (the only intended
// difference from the op path's "operation 'X' ..." wording).
inline rayx_runtime::OpArgs marshal_actor_args(const py::sequence& args,
                                               const std::string& what) {
    rayx_runtime::OpArgs targs;
    targs.reserve(static_cast<std::size_t>(py::len(args)));
    std::size_t ai = 0;
    for (py::handle h : args) {
        if (py::isinstance<py::bool_>(h)) {
            throw std::invalid_argument(what + " argument " +
                std::to_string(ai) + " must not be bool");
        } else if (py::isinstance<py::int_>(h)) {
            targs.emplace_back(h.cast<std::int64_t>());
        } else if (py::isinstance<py::float_>(h)) {
            targs.emplace_back(h.cast<double>());
        } else {
            throw std::invalid_argument(what + " argument " +
                std::to_string(ai) +
                " has an unsupported type (expected int or float)");
        }
        ++ai;
    }
    return targs;
}

// Build the service-slot closure for one ACTOR METHOD call. The analogue of
// make_op_task, but it also carries the actor's native state. CAPTURE CONTRACT (the
// load-bearing invariant from the actor design note + HPX audit): capture the
// shared_ptr<ActorState>, the OpArgs, the actor-id string, and the MethodFn ALL BY
// VALUE. The shared_ptr keeps the state alive for the whole method body even if the
// owning ActorRecord is dropped (the body holds its OWN refcount), so state is freed
// only after the worker has joined AND every in-flight body's closure copy is gone --
// no use-after-free if actors_ is cleared at shutdown. NEVER capture an ActorState&,
// a reference into actors_, or anything whose lifetime is tied to an ActorRecord.
// The body downcasts state defensively via the registered method (as_counter) and
// maps a throw to a status="failed" row, exactly as make_op_task does for ops; no
// Python object is created on the HPX worker, and value/row separation is preserved.
inline rayx_runtime::RuntimeLane::OpTask make_method_task(
        rayx_runtime::MethodFn fn,
        std::shared_ptr<rayx_runtime::ActorState> state,
        rayx_runtime::OpArgs args,
        std::string actor) {
    return [fn = std::move(fn), state = std::move(state),
            args = std::move(args), actor = std::move(actor)](
               const rayx_runtime::StopCheckpoint& stop)
               -> rayx_runtime::RuntimeResult {
        rayx_runtime::RuntimeResult r;
        r.actor_id = actor;
        r.start_ns = rayhpx::now_ns();
        try {
            rayx_runtime::OpOutcome o = fn(*state, args, stop);
            r.value = o.value;
            r.has_value = o.has_value;
            r.status = o.status;
            r.error = o.error;
        } catch (const std::exception& e) {
            // Method exception (incl. defensive as_counter wrong-tag) -> failed row.
            r.status = "failed";
            r.error = e.what();
            r.has_value = false;
        } catch (...) {
            r.status = "failed";
            r.error = "actor method failed with a non-std::exception";
            r.has_value = false;
        }
        r.end_ns = rayhpx::now_ns();
        return r;
    };
}

// ---- endpoint->Runtime bridge slot (v1) ---------------------------------
//
// Process-global handle the endpoint transport (accept thread) uses to find the live
// Runtime and dispatch one fixed registered op. EXPERIMENTAL plumbing; proves the seam,
// NOT HPX value. The drain gate (one mutex + condition_variable) makes Runtime shutdown
// race-safe against an in-flight bridge request. See docs/design/endpoint_runtime_bridge.md.
class RuntimeEngine;  // forward decl: RuntimeBridge holds a RuntimeEngine* (dispatch target)

// Tiny co-owned handle. The raw engine pointer is safe to dereference ONLY while a request
// is counted in_flight: the drain gate guarantees the engine outlives every in-flight
// dispatch (shutdown waits for in_flight==0 before tearing down lanes / the engine).
struct RuntimeBridge {
    RuntimeEngine* engine;
};

// Single-mutex + condvar slot. All four pieces of state live under `mu_`; no HPX task and
// no owner-mutex holder ever touches it, and it is never held across run_as_hpx_thread or
// future::get(). publish() (Runtime ctor, after lanes are live) clears any prior draining
// flag; drain_and_unpublish() (Runtime shutdown, BEFORE lane teardown) blocks new requests
// and waits for the in-flight one to finish.
class RuntimeBridgeSlot {
public:
    static RuntimeBridgeSlot& instance() {
        static RuntimeBridgeSlot s;
        return s;
    }

    void publish(std::shared_ptr<RuntimeBridge> b) {
        std::lock_guard<std::mutex> lk(mu_);
        bridge_ = std::move(b);
        draining_ = false;
    }

    // Begin a request. On success returns the bridge and has incremented in_flight (the
    // caller MUST call release() exactly once). On failure returns nullptr and sets
    // out_status to CALL_RUNTIME_ABSENT / CALL_RUNTIME_DRAINING.
    std::shared_ptr<RuntimeBridge> acquire(int& out_status) {
        std::lock_guard<std::mutex> lk(mu_);
        if (!bridge_) {
            out_status = draining_ ? rayx_endpoint::CALL_RUNTIME_DRAINING
                                   : rayx_endpoint::CALL_RUNTIME_ABSENT;
            return nullptr;
        }
        ++in_flight_;
        return bridge_;
    }

    void release() {
        std::lock_guard<std::mutex> lk(mu_);
        if (in_flight_ > 0 && --in_flight_ == 0) cv_.notify_all();
    }

    // Runtime shutdown: stop admitting new requests, unpublish so new acquires see
    // unavailable, then wait for the (at most one, serial-listener) in-flight request to
    // finish. Returns only once no bridge dispatch is in flight, so the caller may then
    // tear down lanes / the engine safely.
    void drain_and_unpublish() {
        std::unique_lock<std::mutex> lk(mu_);
        draining_ = true;
        bridge_.reset();
        cv_.wait(lk, [this] { return in_flight_ == 0; });
    }

private:
    RuntimeBridgeSlot() = default;
    std::mutex mu_;
    std::condition_variable cv_;
    std::shared_ptr<RuntimeBridge> bridge_;  // published by the live Runtime, else null
    bool draining_ = false;
    int in_flight_ = 0;
};

// Process-singleton runtime engine. Attaches to the shared ProcessHpxOwner as RUNTIME:
// it is the HPX owner while it lives (Engine and Runtime stay mutually exclusive; an
// Endpoint, being HPX-free, may coexist). Slice 1 owns N HPX-native RuntimeLanes (a single
// HPX-thread FIFO worker each), round-robins submissions across them, and exposes per-lane
// observability via lane_stats().
class RuntimeEngine {
public:
    RuntimeEngine(int hpx_threads, int num_lanes,
                  int max_queue_depth_per_lane,
                  bool nonblocking_op_lanes = false,
                  int max_inflight_per_lane = 1) {
        if (hpx_threads < 1)
            throw std::invalid_argument("hpx_threads must be >= 1");
        if (num_lanes < 1)
            throw std::invalid_argument("num_lanes must be >= 1");
        // -1 (sentinel) = unbounded (default); >= 1 = per-lane bounded admission.
        // The Python facade validates None/positive-int and passes -1 for None; the
        // <1 (non-sentinel) guard here is a defensive backstop, mirroring Engine.
        if (max_queue_depth_per_lane != -1 && max_queue_depth_per_lane < 1)
            throw std::invalid_argument(
                "max_queue_depth_per_lane must be -1 (unbounded) or >= 1");
        // EXPERIMENTAL opt-in: non-blocking OPERATION lanes. Default false keeps the
        // serial-blocking lane (the stable anchor). max_inflight_per_lane only applies
        // when enabled and must be >= 1 (the facade validates; this is a backstop).
        if (nonblocking_op_lanes && max_inflight_per_lane < 1)
            throw std::invalid_argument(
                "max_inflight_per_lane must be >= 1 when nonblocking_op_lanes is set");
        nonblocking_op_lanes_ = nonblocking_op_lanes;
        max_inflight_per_lane_ = (max_inflight_per_lane < 1) ? 1
                                                             : max_inflight_per_lane;
        max_qd_per_lane_ = max_queue_depth_per_lane;
        // Runtime is a SINGLETON HPX owner: attach starts HPX iff no Engine/Runtime holds
        // the process (an Endpoint may coexist). Detach on partial-construction failure.
        ProcessHpxOwner::instance().attach(HpxAttachKind::Runtime, hpx_threads);
        // Build the lanes ON an HPX thread: each RuntimeLane ctor spawns an
        // hpx::thread worker, so the runtime must be up and the spawn must happen
        // on an HPX thread. On a partial-construction failure, stop/join whatever
        // lanes already exist (same HPX-thread teardown), stop HPX, clear the
        // guard, and rethrow -- so a failed ctor leaves no live worker, no started
        // runtime, and no claimed guard.
        try {
            // Operation lanes honour the experimental non-blocking mode; ACTOR lanes
            // (create_actor) are always SerialBlocking (method ordering == state
            // correctness), so this mode is op-lanes-only by construction.
            const auto op_lane_mode =
                nonblocking_op_lanes_
                    ? rayx_runtime::RuntimeLane::LaneMode::AsyncNonBlocking
                    : rayx_runtime::RuntimeLane::LaneMode::SerialBlocking;
            const int op_lane_inflight = max_inflight_per_lane_;
            hpx::run_as_hpx_thread([this, num_lanes, op_lane_mode,
                                    op_lane_inflight]() {
                lanes_.reserve(static_cast<std::size_t>(num_lanes));
                for (int i = 0; i < num_lanes; ++i) {
                    lanes_.push_back(
                        std::make_unique<rayx_runtime::RuntimeLane>(
                            "rt-hpx-", op_lane_mode, op_lane_inflight));
                }
            });
        } catch (...) {
            try {
                hpx::run_as_hpx_thread([this]() {
                    for (auto& lane : lanes_)
                        if (lane) lane->stop_and_join();
                    lanes_.clear();
                });
            } catch (...) {
                // best-effort cleanup; do not mask the original failure
            }
            ProcessHpxOwner::instance().detach(HpxAttachKind::Runtime);  // stops HPX
            throw;
        }
        running_ = true;
        // Publish the endpoint->Runtime bridge ONLY now that the lanes are live, so a
        // bridge request can never race a half-constructed engine. Unpublished + drained
        // in shutdown() BEFORE any lane teardown.
        RuntimeBridgeSlot::instance().publish(
            std::make_shared<RuntimeBridge>(RuntimeBridge{this}));
    }

    ~RuntimeEngine() {
        try {
            shutdown();
        } catch (...) {
            // best-effort; never throw from a destructor
        }
    }

    // Native, pybind-FREE registered-op dispatch (the UNBOUNDED enqueue path). Looks the
    // op up, builds the closure (make_op_task), round-robins a lane, and enqueues on an HPX
    // thread; returns the in-flight future. Throws std::invalid_argument on unknown op /
    // wrong arity (callers map that to a typed error). Shared by submit_operation's
    // unbounded path (AFTER Python marshalling) and the endpoint->Runtime bridge (which
    // builds OpArgs from the wire -- NO Python). No py:: anywhere here, so it is safe to
    // call from the endpoint accept thread (which holds no GIL). rr_ is atomic because the
    // bridge adds a SECOND submitting thread (the accept thread) alongside the Python
    // driver -- lane choice is load-balancing only, so a relaxed fetch_add is sufficient.
    //
    // BOUNDED-ADMISSION NOTE: this is the UNBOUNDED enqueue path (plain lane.submit), so it
    // intentionally BYPASSES max_queue_depth_per_lane -- the endpoint->Runtime bridge always
    // uses it. That is acceptable in bridge v1 because the native AF_UNIX listener is serial
    // (one bridge request in flight at a time), so the bridge adds at most one item to a
    // lane and cannot meaningfully exceed the cap. If the bridge listener ever becomes
    // concurrent (multiple in-flight bridge requests), bounded admission must be revisited
    // (e.g. route the bridge through a try_submit path) so the cap is honored.
    hpx::future<rayx_runtime::RuntimeResult> dispatch_op_typed(
            const std::string& op_id, rayx_runtime::OpArgs args,
            std::shared_ptr<rayx_runtime::RuntimeCancelToken>* out_tok) {
        const rayx_runtime::OpEntry* entry = nullptr;
        auto it = rayx_runtime::registry().find(op_id);
        if (it != rayx_runtime::registry().end()) {
            entry = &it->second;
        } else {
            auto hit = rayx_runtime::hpx_registry().find(op_id);
            if (hit != rayx_runtime::hpx_registry().end()) entry = &hit->second;
        }
        if (!entry) throw std::invalid_argument("unknown operation id: " + op_id);
        if (static_cast<int>(args.size()) != entry->arity)
            throw std::invalid_argument(
                "wrong number of arguments for operation: " + op_id);
        const rayx_runtime::OpFn fn = entry->fn;
        const int checkpoint_count = entry->checkpoint_count(args);
        const rayx_runtime::DispatchPolicy policy = entry->policy;
        const std::size_t idx =
            rr_.fetch_add(1, std::memory_order_relaxed) % lanes_.size();
        rayx_runtime::RuntimeLane& lane = *lanes_[idx];
        rayx_runtime::RuntimeLane::OpTask task =
            make_op_task(fn, std::move(args), lane.actor_id());
        return hpx::run_as_hpx_thread(
            [&lane, &task, checkpoint_count, policy, out_tok]() {
                return lane.submit(std::move(task), checkpoint_count, policy, out_tok);
            });
    }

    // Dispatch one registered operation through a round-robin-selected lane.
    // op_id/args are already validated at the Python boundary (unknown id / wrong
    // arity / non-int rejected there); the arity re-check here is a defensive
    // backstop. The op body is packaged as a closure (make_op_task) stamped with
    // the SERVING LANE's actor_id, then enqueued on the lane's hpx::mutex queue --
    // which must happen on an HPX thread, so the enqueue hops via
    // run_as_hpx_thread (mirroring the harness RayxLaneAdapter). The lane's worker
    // runs the closure via hpx::async(exec_, ...).get() (cooperative HPX
    // suspension) in FIFO order; RuntimeFuture.result() later blocks (GIL
    // released) on the returned future. The unbounded path delegates to
    // dispatch_op_typed (shared with the endpoint bridge); the bounded path keeps its
    // own check-and-push try_submit (which returns Python None on a full lane).
    py::object submit_operation(const std::string& op_id,
                                const py::sequence& args) {
        if (!running_) throw std::runtime_error("Runtime is shut down");
        // Look the op up in the HPX-free core registry first, then the HPX-side
        // composed-op registry (fanout_sum). Both hold the SAME OpEntry type; the
        // only difference is whether the body uses HPX internally. op_id/args are
        // already validated at the Python boundary; the arity re-check here is a
        // defensive backstop.
        const rayx_runtime::OpEntry* entry = nullptr;
        auto it = rayx_runtime::registry().find(op_id);
        if (it != rayx_runtime::registry().end()) {
            entry = &it->second;
        } else {
            auto hit = rayx_runtime::hpx_registry().find(op_id);
            if (hit != rayx_runtime::hpx_registry().end()) entry = &hit->second;
        }
        if (!entry) {
            throw std::invalid_argument("unknown operation id: " + op_id);
        }
        if (static_cast<int>(py::len(args)) != entry->arity) {
            throw std::invalid_argument(
                "wrong number of arguments for operation: " + op_id);
        }
        // Typed marshalling (value-model V3): build the OpArgs value channel
        // (vector<variant<int64,double>>) from the Python args. The PUBLIC path is
        // already type-validated by rayx.runtime._validate (per-arg types, int64
        // range, strict-finite double), so this runs on validated values; it is also
        // the native backstop for the raw-_RuntimeEngine bypass. bool is rejected
        // BEFORE int (bool is a Python int subclass); int -> int64; float -> double;
        // anything else -> a clear error. Runs on the Python thread (GIL held); no
        // Python object is constructed on an HPX worker.
        rayx_runtime::OpArgs targs;
        targs.reserve(static_cast<std::size_t>(py::len(args)));
        std::size_t ai = 0;
        for (py::handle h : args) {
            if (py::isinstance<py::bool_>(h)) {
                throw std::invalid_argument("operation '" + op_id + "' argument " +
                    std::to_string(ai) + " must not be bool");
            } else if (py::isinstance<py::int_>(h)) {
                targs.emplace_back(h.cast<std::int64_t>());
            } else if (py::isinstance<py::float_>(h)) {
                targs.emplace_back(h.cast<double>());
            } else {
                throw std::invalid_argument("operation '" + op_id + "' argument " +
                    std::to_string(ai) +
                    " has an unsupported type (expected int or float)");
            }
            ++ai;
        }
        std::shared_ptr<rayx_runtime::RuntimeCancelToken> tok;
        if (max_qd_per_lane_ >= 0) {
            // Bounded admission keeps its own check-and-push path (the unbounded
            // dispatch_op_typed cannot express a Python-None rejection). Checkpoint count
            // from the op's args (square/add/... -> 1; busy_sum -> ceil(n/STRIDE)) arms
            // running-cancellability; the internal Inline/Async DispatchPolicy comes from
            // the registry entry. try_submit check-and-pushes atomically under the lane
            // mutex; std::nullopt means the lane was at/over the cap -> return Python None
            // and the facade raises QueueFullError. rr_ still advances (engine state),
            // matching the harness round-robin-on-reject behavior. rr_ is atomic because
            // the endpoint bridge submits from a second thread; lane choice is load-
            // balancing only, so a relaxed fetch_add is sufficient.
            const rayx_runtime::OpFn fn = entry->fn;
            const int checkpoint_count = entry->checkpoint_count(targs);
            const rayx_runtime::DispatchPolicy policy = entry->policy;
            const std::size_t idx =
                rr_.fetch_add(1, std::memory_order_relaxed) % lanes_.size();
            rayx_runtime::RuntimeLane& lane = *lanes_[idx];
            rayx_runtime::RuntimeLane::OpTask task =
                make_op_task(fn, std::move(targs), lane.actor_id());
            std::optional<hpx::future<rayx_runtime::RuntimeResult>> fut =
                hpx::run_as_hpx_thread(
                    [&lane, &task, checkpoint_count, policy, &tok, this]() {
                        return lane.try_submit(std::move(task), max_qd_per_lane_,
                                               checkpoint_count, policy, &tok);
                    });
            if (!fut) return py::none();  // rejected: no row, no future, no token
            return py::cast(RuntimeFuture(std::move(*fut), std::move(tok)),
                            py::return_value_policy::move);
        }
        // Unbounded path: delegate to the shared native helper (it re-looks-up the entry,
        // selects the round-robin lane, and enqueues). One extra registry lookup vs. the
        // bounded path; negligible and keeps the bridge/Python paths from drifting.
        hpx::future<rayx_runtime::RuntimeResult> fut =
            dispatch_op_typed(op_id, std::move(targs), &tok);
        return py::cast(RuntimeFuture(std::move(fut), std::move(tok)),
                        py::return_value_policy::move);
    }

    // ---- local native actors (C1b) ----------------------------------------

    // Create one local stateful native actor of a registered type, returning its
    // opaque actor_id (the dedicated actor lane's "rt-act-" id). C1b is the native
    // engine plumbing only -- there is no Python ActorHandle / Runtime.create_actor /
    // boundary validation yet (a later slice). The lifecycle follows the corrected
    // algorithm from the design note + HPX audit:
    //
    //   on THIS (external/Python) thread, HPX-free, BEFORE any lane exists:
    //     * registry lookup (unknown type rejected)
    //     * defensive init-arity check + marshal init args (GIL held)
    //     * build the native state via the factory -- a factory failure raises here,
    //       with NO lane yet, so there is nothing to clean up;
    //
    //   inside ONE hpx::run_as_hpx_thread hop (the lane ctor spawns an hpx::thread,
    //   so it + the actor_id read + the actors_ insert must happen on an HPX thread):
    //     * construct RuntimeLane("rt-act-") (worker now live)
    //     * read its actor_id; treat a collision as an error (astronomically unlikely
    //       with 64-bit ids) rather than replacing a live actor
    //     * insert the ActorRecord into actors_ INSIDE the hop
    //     * if ANY step after the worker is live throws (collision, map allocation),
    //       stop_and_join the still-locally-owned lane before unwinding -- so a
    //       joinable hpx::thread is NEVER destroyed by an inert ~RuntimeLane.
    //
    // The lane is moved into actors_ only via NOEXCEPT moves AFTER the one throwing
    // step (the empty-record insertion), so once the record exists the lane can never
    // be stranded; until then it is reachable from the local `lane` for the catch.
    std::string create_actor(const std::string& actor_type,
                             const py::sequence& args) {
        if (!running_) throw std::runtime_error("Runtime is shut down");
        auto it = rayx_runtime::actor_registry().find(actor_type);
        if (it == rayx_runtime::actor_registry().end())
            throw std::invalid_argument("unknown actor type: " + actor_type);
        const rayx_runtime::ActorTypeEntry& entry = it->second;
        // Defensive arity check (full Python-boundary validation is a later slice).
        if (static_cast<std::size_t>(py::len(args)) != entry.init_arg_types.size())
            throw std::invalid_argument(
                "wrong number of init arguments for actor type: " + actor_type);
        // Marshal init args + build native state SYNCHRONOUSLY, HPX-free, on this
        // thread, BEFORE the lane exists -- a factory throw here leaves no worker.
        rayx_runtime::OpArgs init_args =
            marshal_actor_args(args, "actor '" + actor_type + "' init");
        std::shared_ptr<rayx_runtime::ActorState> state =
            entry.factory(init_args);
        // Lane construction + id + map insert, all on an HPX thread (one hop).
        return hpx::run_as_hpx_thread(
            [this, &state, &actor_type]() -> std::string {
                auto lane =
                    std::make_unique<rayx_runtime::RuntimeLane>("rt-act-");
                // Worker is live: any throw below must stop_and_join `lane` before
                // it (and its inert dtor) is destroyed during unwind.
                try {
                    const std::string id = lane->actor_id();
                    std::string atype = actor_type;  // copy now (may throw safely)
                    if (actors_.find(id) != actors_.end())
                        throw std::runtime_error(
                            "actor_id collision on create_actor: " + id);
                    // Only throwing step from here: inserting the empty record
                    // (node allocation). `lane` still owns the worker, so the catch
                    // can join it on failure.
                    ActorRecord& rec = actors_[id];
                    // From here, NOEXCEPT moves only -- nothing can strand the lane.
                    rec.state = std::move(state);
                    rec.actor_type = std::move(atype);
                    rec.lane = std::move(lane);
                    return id;
                } catch (...) {
                    if (lane) lane->stop_and_join();  // join before unwind
                    throw;
                }
            });
    }

    // Dispatch one registered method on an existing actor, returning a _RuntimeFuture
    // (the SAME type op submissions return, so get/wait/as_completed/cancel work
    // unchanged) or Python None when the actor lane is full under a per-lane cap (the
    // facade would raise QueueFullError -- not wired in C1b). Reuses the actor lane's
    // existing submit/try_submit + RuntimeCancelToken machinery; the method body is
    // packaged by make_method_task (capturing the actor's shared_ptr<ActorState> by
    // value) and enqueued on the actor's dedicated lane via run_as_hpx_thread,
    // mirroring submit_operation's submit path exactly.
    py::object call_actor_method(const std::string& actor_id,
                                 const std::string& method_id,
                                 const py::sequence& args) {
        if (!running_) throw std::runtime_error("Runtime is shut down");
        auto ait = actors_.find(actor_id);
        if (ait == actors_.end())
            throw std::invalid_argument("unknown actor_id: " + actor_id);
        ActorRecord& rec = ait->second;
        const rayx_runtime::ActorTypeEntry& entry =
            rayx_runtime::actor_registry().at(rec.actor_type);  // type known-valid
        auto mit = entry.methods.find(method_id);
        if (mit == entry.methods.end())
            throw std::invalid_argument(
                "unknown method '" + method_id + "' for actor type '" +
                rec.actor_type + "'");
        const rayx_runtime::MethodEntry& method = mit->second;
        // Defensive arity check (full Python-boundary validation is a later slice).
        if (static_cast<int>(py::len(args)) != method.arity)
            throw std::invalid_argument(
                "wrong number of arguments for method '" + method_id + "'");
        rayx_runtime::OpArgs targs =
            marshal_actor_args(args, "method '" + method_id + "'");
        const int checkpoint_count = method.checkpoint_count(targs);
        // Internal lane-dispatch policy from the method entry (NOT from
        // checkpoint_count): Inline for instantaneous methods (add/get/reset),
        // Async (default) for the checkpointed busy_get. Same thread-through as
        // the op path; no Python surface, no row/value change.
        const rayx_runtime::DispatchPolicy policy = method.policy;
        rayx_runtime::MethodFn fn = method.fn;  // copied into the closure
        rayx_runtime::RuntimeLane& lane = *rec.lane;
        // Capture-by-value contract (make_method_task): rec.state is copied (refcount
        // ++), then moved into the closure -- the body holds its own shared_ptr.
        rayx_runtime::RuntimeLane::OpTask task =
            make_method_task(std::move(fn), rec.state, std::move(targs),
                             lane.actor_id());
        std::shared_ptr<rayx_runtime::RuntimeCancelToken> tok;
        if (max_qd_per_lane_ >= 0) {
            std::optional<hpx::future<rayx_runtime::RuntimeResult>> fut =
                hpx::run_as_hpx_thread(
                    [&lane, &task, checkpoint_count, policy, &tok, this]() {
                        return lane.try_submit(std::move(task), max_qd_per_lane_,
                                               checkpoint_count, policy, &tok);
                    });
            if (!fut) return py::none();  // rejected: no row/future/token created
            return py::cast(RuntimeFuture(std::move(*fut), std::move(tok)),
                            py::return_value_policy::move);
        }
        hpx::future<rayx_runtime::RuntimeResult> fut =
            hpx::run_as_hpx_thread([&lane, &task, checkpoint_count, policy, &tok]() {
                return lane.submit(std::move(task), checkpoint_count, policy, &tok);
            });
        return py::cast(RuntimeFuture(std::move(fut), std::move(tok)),
                        py::return_value_policy::move);
    }

    // As-completed wait over RuntimeFutures (Slice 2c). Block (GIL released) until
    // at least num_returns of the given _RuntimeFuture objects are ready, then
    // return the indices (into the input list) of ALL currently-ready futures. The
    // caller retires the ones it wants and keeps the rest in flight. A direct
    // mirror of Engine::wait, retargeted to rayx_runtime::RuntimeResult; see that
    // method for the full hpx::wait_some / non-consuming / RAII-restore / GIL
    // rationale. NON-consuming: hpx::wait_some keeps every input future valid; we
    // move them out and back into the SAME RuntimeFuture objects only because
    // hpx::future is move-only and we use the std::vector overload. A failed or
    // cancelled op resolves its future normally (the outcome is encoded in the
    // RuntimeResult, not thrown into the future), so wait treats completed, failed,
    // and cancelled results uniformly as "ready".
    py::list wait(py::list futures, int num_returns) {
        if (!running_) throw std::runtime_error("Runtime is shut down");
        const std::size_t n = futures.size();
        if (n == 0) throw std::invalid_argument("wait() got an empty futures list");
        if (num_returns < 1)
            throw std::invalid_argument("num_returns must be >= 1");
        if (static_cast<std::size_t>(num_returns) > n)
            throw std::invalid_argument("num_returns must be <= len(futures)");

        // Borrow the C++ wrappers (Python retains the objects). A non-_RuntimeFuture
        // entry raises TypeError via pybind's cast; an already-retired future raises
        // a clear error; a duplicate (same underlying _RuntimeFuture twice) is
        // rejected BEFORE any take() below -- otherwise we would move the same
        // hpx::future out twice and corrupt it.
        std::vector<RuntimeFuture*> rfs;
        rfs.reserve(n);
        std::unordered_set<RuntimeFuture*> seen;
        seen.reserve(n);
        for (py::handle h : futures) {
            RuntimeFuture* rf = h.cast<RuntimeFuture*>();
            if (!rf->valid_now()) {
                throw std::runtime_error(
                    "wait() received a RuntimeFuture already retired via result()");
            }
            if (!seen.insert(rf).second) {
                throw std::invalid_argument(
                    "wait() received the same RuntimeFuture more than once; each "
                    "future may appear at most once");
            }
            rfs.push_back(rf);
        }

        // Move each underlying future out into `tmp`, guarded by a RAII Restore that
        // moves them back into the SAME RuntimeFuture objects on EVERY exit path
        // (normal return or an exception out of wait_some), restoring exactly as
        // many as were taken -- so even a partial take is unwound cleanly and a
        // RuntimeFuture is never left moved-from.
        std::vector<hpx::future<rayx_runtime::RuntimeResult>> tmp;
        tmp.reserve(n);
        struct Restore {
            std::vector<RuntimeFuture*>& rfs;
            std::vector<hpx::future<rayx_runtime::RuntimeResult>>& tmp;
            ~Restore() {
                for (std::size_t i = 0; i < tmp.size(); ++i)
                    rfs[i]->put(std::move(tmp[i]));
            }
        } restore{rfs, tmp};
        for (RuntimeFuture* rf : rfs) tmp.push_back(rf->take());

        {
            // Release the GIL ONLY around the blocking HPX wait. No Python objects
            // are touched inside this scope (tmp holds C++ futures); the ready-list
            // construction below runs with the GIL reacquired.
            py::gil_scoped_release release;
            hpx::wait_some(static_cast<std::size_t>(num_returns), tmp);
        }

        py::list ready;
        for (std::size_t i = 0; i < tmp.size(); ++i) {
            if (tmp[i].is_ready()) ready.append(static_cast<int>(i));
        }
        return ready;  // ~Restore() moves the futures back into the RuntimeFutures
    }

    int num_lanes() const { return static_cast<int>(lanes_.size()); }

    // Observability snapshot (debugging only): one dict per lane, in stable lane
    // order, with the lane's queued-but-not-started depth and whether it is in a
    // service slot. Collects each lane's stats ON an HPX thread (the queue size is
    // read under the lane's hpx::mutex); NON-consuming -- touches no future and
    // changes no submit/service semantics. Snapshot only; values can change the
    // instant after this returns. Raises if shut down (lanes destroyed). Not Ray
    // scheduler state, not placement control, not part of the JSONL schema.
    py::list lane_stats() {
        if (!running_) throw std::runtime_error("Runtime is shut down");
        std::vector<rayx_runtime::RuntimeLane::LaneStat> snaps =
            hpx::run_as_hpx_thread([this]() {
                std::vector<rayx_runtime::RuntimeLane::LaneStat> v;
                v.reserve(lanes_.size());
                for (auto& lane : lanes_) v.push_back(lane->stats());
                return v;
            });
        py::list out;  // built with the GIL held (no Python touched in the hop)
        for (auto& s : snaps) {
            py::dict d;
            d["actor_id"] = s.actor_id;
            d["queue_depth"] = s.queue_depth;
            d["active"] = s.active;
            out.append(std::move(d));
        }
        return out;
    }

    // Per-actor observability snapshot (debugging only): the actor's dedicated
    // lane's {actor_id, queue_depth, active} -- the actor analogue of one
    // lane_stats() element, with the SAME three fields and the same semantics
    // (queue_depth counts queued-but-not-started method calls; the in-service
    // call is popped and NOT counted; active is true while a method is in the
    // service slot). NON-consuming and racy: a point-in-time snapshot that
    // touches no future and changes no call/cancel semantics. Reads the lane's
    // stats ON an HPX thread (RuntimeLane::stats takes the lane's hpx::mutex);
    // the dict is built with the GIL held after the hop. lane_stats() above
    // remains op-lanes-only and is NOT changed by this. Unknown actor_id (only
    // reachable via the raw bypass today -- actors live until shutdown -- and
    // via a future release_actor) -> std::invalid_argument -> Python ValueError.
    // Raises if the runtime is shut down, consistent with call_actor_method.
    py::dict actor_stats(const std::string& actor_id) {
        if (!running_) throw std::runtime_error("Runtime is shut down");
        auto it = actors_.find(actor_id);
        if (it == actors_.end())
            throw std::invalid_argument("unknown actor_id: " + actor_id);
        rayx_runtime::RuntimeLane& lane = *it->second.lane;
        rayx_runtime::RuntimeLane::LaneStat s =
            hpx::run_as_hpx_thread([&lane]() { return lane.stats(); });
        py::dict d;
        d["actor_id"] = s.actor_id;
        d["queue_depth"] = s.queue_depth;
        d["active"] = s.active;
        return d;
    }

    // exp44 debug-only structural witness for the LAST barrier_fanin execution:
    // {seq, observed_os_workers, leaves_requested, arrived_count, released_count,
    // opener, reduction_after_all_leaves, ordering_violations, clean_exit,
    // watchdog_opened, joined_count, max_simultaneously_suspended_leaves}. Reads the
    // mutex-guarded process-global slot (no torn read; "racy" only means a reader may
    // see a stale/cross-call value). barrier_fanin is the ONE side-effecting registry
    // op; this accessor exposes ONLY that witness and does NOT touch lane_stats(),
    // RuntimeFuture, or the JSONL schema. Single-in-flight contract for tests/experiment
    // (one barrier_fanin at a time -> `seq` identifies the execution). Raises if the
    // runtime is shut down, consistent with the other snapshot accessors. NOT scheduler
    // state, NOT a synchronization primitive, NOT placement control, NOT a perf metric;
    // max_simultaneously_suspended_leaves is COORDINATED SUSPENSION, never parallelism.
    py::dict barrier_fanin_witness() {
        if (!running_) throw std::runtime_error("Runtime is shut down");
        rayx_runtime::BarrierFaninWitness w =
            rayx_runtime::read_barrier_fanin_witness();
        py::dict d;
        d["seq"] = w.seq;
        d["observed_os_workers"] = w.observed_os_workers;
        d["leaves_requested"] = w.leaves_requested;
        d["arrived_count"] = w.arrived_count;
        d["released_count"] = w.released_count;
        d["opener"] = w.opener;
        d["reduction_after_all_leaves"] = w.reduction_after_all_leaves;
        d["ordering_violations"] = w.ordering_violations;
        d["clean_exit"] = w.clean_exit;
        d["watchdog_opened"] = w.watchdog_opened;
        d["joined_count"] = w.joined_count;
        d["max_simultaneously_suspended_leaves"] =
            w.max_simultaneously_suspended_leaves;
        return d;
    }

    // exp47 debug-only structural witness for the LAST overlap_probe execution:
    // {seq, mode, observed_os_workers, arms_launched, arms_completed, max_in_flight,
    // both_in_flight, per_arm_enter_seq, per_arm_leave_seq, per_arm_chunk_event_count,
    // per_arm_worker_ids, per_arm_worker_ids_overflowed, ordering_violations, clean_exit,
    // classification, event_count, event_trace_overflowed, events}. overlap_probe is the
    // second side-effecting registry op (after barrier_fanin); this accessor exposes ONLY
    // that witness and does NOT touch lane_stats(), RuntimeFuture, or the JSONL schema.
    // Mutex-guarded snapshot (no torn read; "racy" only means stale/cross-call);
    // single-in-flight contract for tests/experiment (one overlap_probe at a time -> `seq`
    // identifies the execution). Raises if the runtime is shut down. NOT scheduler state,
    // NOT a synchronization primitive, NOT placement control, NOT a perf metric;
    // "in flight" means active within the bracketed arm compute (not necessarily executing
    // CPU instructions at the same instant), and "worker_parallel" means only "distinct
    // workers observed", never proof of speedup/parallel execution.
    py::dict overlap_witness() {
        if (!running_) throw std::runtime_error("Runtime is shut down");
        rayx_runtime::OverlapWitness w = rayx_runtime::read_overlap_witness();
        py::dict d;
        d["seq"] = w.seq;
        d["mode"] = w.mode;
        d["observed_os_workers"] = w.observed_os_workers;
        d["arms_launched"] = w.arms_launched;
        d["arms_completed"] = w.arms_completed;
        d["max_in_flight"] = w.max_in_flight;
        d["both_in_flight"] = w.both_in_flight;
        d["ordering_violations"] = w.ordering_violations;
        d["clean_exit"] = w.clean_exit;
        d["classification"] = w.classification;
        d["event_count"] = w.event_count;
        d["event_trace_overflowed"] = w.event_trace_overflowed;
        py::list enter_seq, leave_seq, chunk_counts, worker_ids, worker_overflow;
        for (int j = 0; j < rayx_runtime::OVERLAP_ARMS; ++j) {
            enter_seq.append(w.per_arm_enter_seq[j]);
            leave_seq.append(w.per_arm_leave_seq[j]);
            chunk_counts.append(w.per_arm_chunk_event_count[j]);
            py::list ids;
            for (long long x : w.per_arm_worker_ids[j]) ids.append(x);
            worker_ids.append(ids);
            worker_overflow.append(w.per_arm_worker_ids_overflowed[j]);
        }
        d["per_arm_enter_seq"] = enter_seq;
        d["per_arm_leave_seq"] = leave_seq;
        d["per_arm_chunk_event_count"] = chunk_counts;
        d["per_arm_worker_ids"] = worker_ids;
        d["per_arm_worker_ids_overflowed"] = worker_overflow;
        py::list events;
        for (const auto& e : w.events) {
            py::dict ed;
            ed["arm_id"] = e.arm_id;
            ed["phase"] = e.phase;
            ed["seq"] = e.seq;
            ed["worker_id"] = e.worker_id;
            events.append(ed);
        }
        d["events"] = events;
        return d;
    }

    // Release ONE local native actor: shutdown()'s actor teardown scoped to a
    // single record, same fixed order while HPX is still running (the design
    // note's lifecycle contract, steps 1->2->3):
    //
    //   1. find the record while the runtime is running. Unknown actor_id ->
    //      std::invalid_argument -> Python ValueError: the raw-bypass backstop
    //      (the facade's released-handle flag raises its clearer RuntimeError
    //      before this is ever reached on the public path).
    //   2. ONE GIL-released hpx::run_as_hpx_thread hop: cancel_pending() (every
    //      queued method call cancels now -- the worker pops-and-skips it; an
    //      in-flight checkpointed method is asked to stop at its NEXT chunk
    //      boundary; an in-flight instantaneous method just completes), then
    //      stop_and_join() (drains, fulfilling EVERY promise -- no broken
    //      future; bounded by one checkpoint stride, never a full op). The GIL
    //      is released because the drain may block on a boundary; no Python
    //      object is touched inside the hop. Synchronous by design: when this
    //      returns, the actor lane worker has joined.
    //   3. only AFTER the worker joined, erase the record -- dropping the
    //      shared_ptr<ActorState> (an in-flight body held its own copy) and the
    //      joined lane. Rows already produced keep their stamped rt-act- id
    //      (ids are string copies in each row; nothing here mutates them).
    //
    // Single-driver assumption as everywhere else in this engine: no map lock;
    // concurrent release/call/stats/shutdown from multiple Python threads is
    // not supported and not claimed. This is LOCAL explicit release only -- not
    // ray.kill, no distributed ownership, no refcounting, no GC hook.
    void release_actor(const std::string& actor_id) {
        if (!running_) throw std::runtime_error("Runtime is shut down");
        auto it = actors_.find(actor_id);
        if (it == actors_.end())
            throw std::invalid_argument("unknown actor_id: " + actor_id);
        rayx_runtime::RuntimeLane& lane = *it->second.lane;
        {
            py::gil_scoped_release release;
            hpx::run_as_hpx_thread([&lane]() {
                lane.cancel_pending();
                lane.stop_and_join();
            });
        }
        actors_.erase(it);  // state dropped only after the worker joined
    }

    void shutdown() {
        if (!running_) return;
        running_ = false;
        // Drain-gate FIRST (before any lane teardown): stop admitting bridge requests,
        // unpublish so new ones see "runtime unavailable", and BLOCK until the at-most-one
        // in-flight bridge dispatch finishes. Only then is it safe to cancel/join/destroy
        // the lanes the in-flight request may be using. The GIL is RELEASED around the wait
        // so a concurrent bridge worker (which itself runs GIL-free) can finish and so other
        // Python threads are not blocked; the slot's own mutex (never an HPX/owner lock) is
        // the only lock held. With v1's instantaneous bridge ops the wait is bounded by one
        // op's service time.
        {
            py::gil_scoped_release release;
            RuntimeBridgeSlot::instance().drain_and_unpublish();
        }
        // Runtime shutdown CANCELS outstanding work, then drains -- an intentional
        // difference from the harness drain-to-completion (registered operations
        // can be long-running, so draining a queued/in-flight busy_sum to its end
        // would make teardown block for the whole op). cancel_pending() cancels
        // every queued token (the worker then skips them) and the in-flight token
        // (a running busy_sum stops at its next checkpoint), so stop_and_join drains
        // promptly -- bounded by one checkpoint stride, not the full op -- while
        // still fulfilling every promise. The GIL is RELEASED around the hop because
        // the drain may block on a checkpoint; the hop locks each lane's hpx::mutex
        // / joins its hpx::thread, so it runs on an HPX thread while HPX is still up.
        // Lanes are cleared BEFORE stop_process_hpx(); the guard is cleared LAST.
        if (!actors_.empty() || !lanes_.empty()) {
            py::gil_scoped_release release;
            hpx::run_as_hpx_thread([this]() {
                // Actor lanes FIRST (before the op lanes, before stop_process_hpx):
                // cancel every queued/in-flight method, stop+join each actor lane,
                // THEN drop the records. actors_.clear() releases each ActorRecord's
                // shared_ptr<ActorState>; doing it ONLY after every actor worker has
                // joined means no in-flight method body can touch freed state (the
                // body also holds its own shared_ptr copy, so the order is safe even
                // mid-drain). Same cancel-then-drain teardown the op lanes use.
                for (auto& kv : actors_)
                    if (kv.second.lane) kv.second.lane->cancel_pending();
                for (auto& kv : actors_)
                    if (kv.second.lane) kv.second.lane->stop_and_join();
                actors_.clear();
                // Then the op lanes (existing behavior, unchanged).
                for (auto& lane : lanes_)
                    if (lane) lane->cancel_pending();
                for (auto& lane : lanes_)
                    if (lane) lane->stop_and_join();
                lanes_.clear();
            });
        }
        // Lanes are drained/joined above (while HPX is up); detach now stops HPX as the
        // last HPX-needing owner (an Endpoint, being HPX-free, never keeps HPX alive).
        ProcessHpxOwner::instance().detach(HpxAttachKind::Runtime);
    }

private:
    std::vector<std::unique_ptr<rayx_runtime::RuntimeLane>> lanes_;
    // Atomic: the endpoint->Runtime bridge submits from the accept thread, so this round-
    // robin cursor is read/advanced by two threads. Lane choice is load-balancing only
    // (not correctness), so relaxed ordering is sufficient.
    std::atomic<std::size_t> rr_{0};
    // Per-lane bounded-admission cap: -1 = unbounded (default); >= 1 = max
    // queued-but-not-started requests admitted per lane (see submit_operation).
    int max_qd_per_lane_ = -1;
    // EXPERIMENTAL: when set, OPERATION lanes are built AsyncNonBlocking with
    // max_inflight_per_lane_ in-flight Async ops each (actor lanes stay serial).
    bool nonblocking_op_lanes_ = false;
    int max_inflight_per_lane_ = 1;
    bool running_ = false;

    // ---- local native actors (C1b) ----------------------------------------
    // One dedicated RuntimeLane per actor (the lane IS the actor's FIFO mailbox /
    // serialization domain), the actor's native state, and its registered type id.
    // SINGLE-DRIVER assumption (same as the rest of the runtime): the map is
    // read/written WITHOUT an internal lock; concurrent create_actor / call /
    // shutdown from multiple Python threads is NOT supported and not claimed.
    struct ActorRecord {
        std::unique_ptr<rayx_runtime::RuntimeLane> lane;
        std::shared_ptr<rayx_runtime::ActorState> state;
        std::string actor_type;
    };
    std::unordered_map<std::string, ActorRecord> actors_;
};

// ---- hpx_smoke (retained debug helper) ----------------------------------

int run_hpx_trivial() {
    char arg0[] = "rayx";
    char* argv[] = {arg0, nullptr};
    int argc = 1;
    hpx::init_params params;
    if (!hpx::start(nullptr, argc, argv, params)) {
        throw std::runtime_error("hpx::start failed");
    }
    int value = hpx::run_as_hpx_thread(
        []() -> int { return hpx::async([]() { return 42; }).get(); });
    hpx::post([]() { hpx::finalize(); });
    hpx::stop();
    return value;
}

py::dict hpx_smoke() {
    int value = 0;
    {
        py::gil_scoped_release release;
        value = run_hpx_trivial();
    }
    py::dict d;
    d["status"] = "ok";
    d["value"] = value;
    return d;
}

// ===== Experimental endpoint seam: Ray bootstrap + endpoint identity + A1 transport ====
//
// EXPERIMENTAL, NARROW, additive. NOT a fabric, NOT parcelport / AGAS, NOT multi-node,
// NO performance claim. A1 adds PLAIN NATIVE LOCAL IPC (AF_UNIX, endpoint_transport.hpp)
// so that, with Endpoint(transport=True), connect(peer).ping(nonce) can cross the process
// boundary on ONE node. HPX is NOT the delivery mechanism, NOT serving the socket, and
// NOT computing the ping: the peer-specific transform is computed INLINE on both the
// same-process registry path and the cross-process transport path.
//
// VARIANT 2 LIFECYCLE: an Endpoint is HPX-FREE. It does NOT start, finalize, or stop HPX.
// Construction attaches to the ProcessHpxOwner as an ENDPOINT (policy/bookkeeping only:
// it coexists with a Runtime and other Endpoints, but is rejected while an Engine is
// active) and registers a seam; close() unregisters the seam and detaches. HPX is owned
// solely by Runtime/Engine. So a process can hold BOTH an Endpoint and a Runtime: the
// Runtime owns HPX; the Endpoint just runs native IPC alongside it. (Endpoint -> Runtime
// request dispatch is FUTURE work; this slice only establishes coexistence.)
//
// SYNCHRONIZATION: the owner (ProcessHpxOwner) serializes attach/detach and the HPX
// lifecycle. The transport listener refcount has its own small mutex below. The CPython
// GIL serializes Python construction calls; no free-threaded / no-GIL claim.

// Forward decls (transport listener lifecycle is defined just below).
std::mutex& endpoint_transport_mu();
int& endpoint_transport_rc();

// rayx.endpoint.shutdown(): Variant 2 retains this public symbol for API/fixture
// compatibility ONLY. Endpoint no longer owns HPX, so this NEVER touches HPX and NEVER
// raises. Normal endpoint cleanup is per-Endpoint.close(). As a best-effort idempotent
// helper, when no endpoints remain it clears closed-id tombstones and, defensively, stops
// a transport listener if one is somehow still running with a zero transport refcount.
void endpoint_shutdown() {
    if (ProcessHpxOwner::instance().endpoint_count() != 0)
        return;  // endpoints still live -> nothing to do (close them via Endpoint.close())
    std::lock_guard<std::mutex> lk(endpoint_transport_mu());
    if (endpoint_transport_rc() == 0 &&
        rayx_endpoint::transport_listener_running())
        rayx_endpoint::transport_listener_stop();
    rayx_endpoint::EndpointRegistry::instance().clear_tombstones();
}

// ---- A1 transport listener lifecycle (process-local; independent of HPX) -------------
// One AF_UNIX listener per process, shared by all transport-enabled endpoints. Started
// lazily by the FIRST transport endpoint and stopped when the LAST one closes. Guarded by
// its own small mutex (no relation to HPX -- the listener is a plain std::thread).
std::mutex& endpoint_transport_mu() {
    static std::mutex m;
    return m;
}
int& endpoint_transport_rc() {
    static int rc = 0;
    return rc;
}

// Start the listener if this is the first transport endpoint; return its socket path.
// Throws (mapped to EndpointError in Python) if the listener cannot start; the refcount
// is left untouched on failure.
std::string endpoint_transport_acquire() {
    std::lock_guard<std::mutex> lk(endpoint_transport_mu());
    std::string path = rayx_endpoint::transport_listener_start();  // throws on failure
    ++endpoint_transport_rc();
    return path;
}

// Drop one transport share; stop the listener when the last transport endpoint closes.
void endpoint_transport_release() {
    std::lock_guard<std::mutex> lk(endpoint_transport_mu());
    if (endpoint_transport_rc() > 0 && --endpoint_transport_rc() == 0)
        rayx_endpoint::transport_listener_stop();
}

// Bridge dispatch (v1): map a closed op-code to a registered Runtime op, build typed
// OpArgs from the wire, dispatch through the drain-gated bridge slot, block for the result
// (on the CALLING thread -- the accept std::thread for a remote call, the Python thread for
// the same-process path), and marshal a typed CallResult. It NEVER throws (returns a
// CallResult status instead) and NEVER touches Python, so it is safe both on the accept
// thread (no GIL) and behind a GIL release. Installed as the transport's CALL_OP handler
// AND called directly for the same-process path. v1 op codes are int64-only and
// instantaneous (no parking/checkpointed/composed ops).
rayx_endpoint::CallResult bridge_dispatch(
        int op_code, const std::vector<rayx_endpoint::CallOpArg>& args) {
    using namespace rayx_endpoint;
    try {
        std::string op_id;
        int arity = 0;
        switch (op_code) {
            case BRIDGE_OP_SQUARE:     op_id = "square";     arity = 1; break;
            case BRIDGE_OP_ADD:        op_id = "add";        arity = 2; break;
            // Bridge v2: composed-op structural bridge. fanout_sum's body uses nested HPX
            // composition (hpx::async + hpx::when_all). This proves the bridged path reaches
            // a body that runs in a valid HPX-thread context and executes that composition,
            // returning the correct typed result -- NOT scheduling/overlap/parallelism value.
            case BRIDGE_OP_FANOUT_SUM: op_id = "fanout_sum"; arity = 2; break;
            default: return {CALL_OP_UNKNOWN, 0, 0};
        }
        if (static_cast<int>(args.size()) != arity) return {CALL_OP_BADARGS, 0, 0};
        // Bridge v2 shutdown/drain bound (NOT a perf knob): cap fanout_sum's `n` so the
        // non-cancellable op cannot stretch the in-flight drain. Only the UPPER bound lives
        // here; n < 0 and parts range are delegated to the op body (-> CALL_OP_FAILED).
        if (op_code == BRIDGE_OP_FANOUT_SUM && args[0].tag == 0 &&
            args[0].bits > BRIDGE_FANOUT_N_MAX) {
            return {CALL_OP_BADARGS, 0, 0};
        }
        rayx_runtime::OpArgs oa;
        oa.reserve(args.size());
        for (const auto& a : args) {
            if (a.tag != 0) return {CALL_OP_BADARGS, 0, 0};  // bridge args are int64-only
            oa.emplace_back(static_cast<std::int64_t>(a.bits));
        }
        int st = 0;
        std::shared_ptr<RuntimeBridge> bridge =
            RuntimeBridgeSlot::instance().acquire(st);
        if (!bridge) return {st, 0, 0};  // CALL_RUNTIME_ABSENT / CALL_RUNTIME_DRAINING
        struct ReleaseGuard {
            ~ReleaseGuard() { RuntimeBridgeSlot::instance().release(); }
        } release_guard;
        // Enqueue (enters HPX) + block on the result OUTSIDE the slot lock. The caller is
        // an external OS thread (accept thread or Python thread), so blocking it does not
        // consume an HPX worker; HPX still fulfills the future even at hpx_threads=1.
        hpx::future<rayx_runtime::RuntimeResult> fut =
            bridge->engine->dispatch_op_typed(op_id, std::move(oa), nullptr);
        rayx_runtime::RuntimeResult r = fut.get();
        if (r.status == "completed" && r.has_value) {
            CallResult cr;
            cr.status = CALL_OK;
            std::visit([&cr](const auto& v) {
                using T = std::decay_t<decltype(v)>;
                if constexpr (std::is_same_v<T, std::int64_t>) {
                    cr.tag = 0;
                    cr.bits = v;
                } else {
                    cr.tag = 1;
                    double d = static_cast<double>(v);
                    std::int64_t bits = 0;
                    std::memcpy(&bits, &d, sizeof bits);
                    cr.bits = bits;
                }
            }, r.value);
            return cr;
        }
        return {CALL_OP_FAILED, 0, 0};  // failed / cancelled op body
    } catch (const std::invalid_argument&) {
        return {CALL_OP_BADARGS, 0, 0};  // unknown op-id / wrong arity from dispatch
    } catch (const std::exception&) {
        return {CALL_INTERNAL, 0, 0};
    } catch (...) {
        return {CALL_INTERNAL, 0, 0};
    }
}

// A connection to a peer seam. Two flavors behind ONE Python-facing type:
//   * LOCAL (same process): holds a weak_ptr to the peer seam; ping() computes the
//     peer-specific transform INLINE (NO HPX). A closed/destroyed peer -> PEER_CLOSED.
//   * REMOTE (cross-process, same node): a PROBED REMOTE ENDPOINT HANDLE. connect()
//     probed reachability once; this object then holds only the peer's AF_UNIX path + id
//     + a deadline. It does NOT own a persistent fd -- each ping() performs ONE bounded,
//     one-shot AF_UNIX round trip (dial -> send -> recv -> close). Still NO HPX.
// Copyable by value (py::cast): remote state is just strings + a timeout; no owned fd is
// held between pings, so there is nothing to double-close.
class _Connection {
public:
    explicit _Connection(std::weak_ptr<rayx_endpoint::EndpointSeam> peer)
        : peer_(std::move(peer)) {}
    _Connection(std::string sock_path, std::string peer_id, long timeout_ms)
        : remote_(true), path_(std::move(sock_path)), peer_id_(std::move(peer_id)),
          timeout_ms_(timeout_ms) {}

    // Returns (status, value): 0 ok / 1 peer-closed / 2 conn-closed / 3 not-found /
    // 4 proto-err / 5 timeout (see rayx_endpoint::PingStatus).
    std::pair<int, std::int64_t> ping(std::int64_t nonce) {
        if (closed_) return {rayx_endpoint::PING_CONN_CLOSED, 0};
        if (remote_) {
            py::gil_scoped_release release;  // blocking IPC off the GIL
            return rayx_endpoint::ping_remote(path_, peer_id_, nonce, timeout_ms_);
        }
        auto s = peer_.lock();
        if (!s || s->closed.load(std::memory_order_relaxed))
            return {rayx_endpoint::PING_PEER_CLOSED, 0};
        return {rayx_endpoint::PING_OK, s->on_ping(nonce)};  // INLINE, no HPX
    }

    // Bridge v1: call one fixed registered Runtime op in the peer's process, returning a
    // (status, value) tuple. status is a rayx_endpoint::CallStatus; value is a Python int
    // (tag 0) / float (tag 1) on CALL_OK, else None. NOT public API -- reached only via the
    // private Connection._call_op. The same-process path dispatches in-process through the
    // SAME drain-gated bridge (no socket); the remote path is one AF_UNIX round trip.
    py::object call_op(int op_code, const std::vector<std::int64_t>& args) {
        std::vector<rayx_endpoint::CallOpArg> av;
        av.reserve(args.size());
        for (std::int64_t v : args)
            av.push_back(rayx_endpoint::CallOpArg{0, v});
        rayx_endpoint::CallResult cr;
        if (closed_) {
            cr = {rayx_endpoint::CALL_CONN_CLOSED, 0, 0};
        } else if (remote_) {
            py::gil_scoped_release release;  // blocking IPC off the GIL
            cr = rayx_endpoint::call_op_remote(path_, peer_id_, op_code, av, timeout_ms_);
        } else {
            auto s = peer_.lock();
            if (!s || s->closed.load(std::memory_order_relaxed)) {
                cr = {rayx_endpoint::CALL_PEER_CLOSED, 0, 0};
            } else {
                py::gil_scoped_release release;  // dispatch blocks on the Runtime future
                cr = bridge_dispatch(op_code, av);
            }
        }
        py::object value = py::none();
        if (cr.status == rayx_endpoint::CALL_OK) {
            if (cr.tag == 0) {
                value = py::int_(cr.bits);
            } else {
                double d = 0;
                std::memcpy(&d, &cr.bits, sizeof d);
                value = py::float_(d);
            }
        }
        return py::make_tuple(cr.status, value);
    }

    void close() { closed_ = true; }

private:
    std::weak_ptr<rayx_endpoint::EndpointSeam> peer_;  // local path
    bool remote_ = false;
    std::string path_;     // remote: peer AF_UNIX socket path
    std::string peer_id_;  // remote: target endpoint id (carried on the wire)
    long timeout_ms_ = 0;  // remote: single deadline for dial+send+recv
    bool closed_ = false;
};

// One process-local endpoint (Variant 2: HPX-FREE). Construction attaches to the owner as
// an ENDPOINT (policy only -- rejected while an Engine is active; coexists with a Runtime),
// mints an identity, registers a seam, and optionally joins the transport listener.
// close() unregisters the seam (tombstone), releases the transport share, and detaches;
// it NEVER touches HPX. When the last endpoint detaches, closed-id tombstones are cleared.
class _Endpoint {
public:
    explicit _Endpoint(bool transport) : transport_(transport) {
        ProcessHpxOwner::instance().attach(HpxAttachKind::Endpoint, 0);  // policy only
        try {
            const std::string id = rayx_endpoint::make_endpoint_id();
            seam_ = std::make_shared<rayx_endpoint::EndpointSeam>(
                id, static_cast<long>(::getpid()));
            rayx_endpoint::EndpointRegistry::instance().register_seam(seam_);
            if (transport_)
                transport_addr_ = endpoint_transport_acquire();  // may throw
        } catch (...) {
            if (seam_)
                rayx_endpoint::EndpointRegistry::instance().unregister(
                    seam_->endpoint_id);
            ProcessHpxOwner::instance().detach(HpxAttachKind::Endpoint);
            throw;
        }
    }
    ~_Endpoint() { close(); }

    _Endpoint(const _Endpoint&) = delete;
    _Endpoint& operator=(const _Endpoint&) = delete;

    std::string id() const { return seam_->endpoint_id; }
    long pid() const { return seam_->pid; }
    int proto_version() const { return seam_->proto_version; }
    bool closed() const { return closed_; }
    std::string transport_kind() const { return transport_ ? "unix" : "none"; }
    std::string transport_addr() const { return transport_addr_; }

    void close() {
        if (closed_) return;  // idempotent
        closed_ = true;
        seam_->closed.store(true, std::memory_order_relaxed);
        rayx_endpoint::EndpointRegistry::instance().unregister(seam_->endpoint_id);
        if (transport_)
            endpoint_transport_release();  // stops the listener at the last endpoint
        auto& owner = ProcessHpxOwner::instance();
        owner.detach(HpxAttachKind::Endpoint);  // HPX-free: never stops HPX
        // Last endpoint gone -> drop closed-id tombstones (bounds growth; the transport
        // listener is also down by now, so a stale peer dial fails -> Closed anyway).
        if (owner.endpoint_count() == 0)
            rayx_endpoint::EndpointRegistry::instance().clear_tombstones();
    }

private:
    std::shared_ptr<rayx_endpoint::EndpointSeam> seam_;
    bool transport_ = false;
    std::string transport_addr_;  // AF_UNIX socket path when transport_ is true
    bool closed_ = false;
};

// Module-level connect: resolve `peer_id` in the PROCESS-LOCAL registry. The Python
// layer has already rejected cross-process peers (pid/host mismatch ->
// EndpointUnreachableError) and proto mismatches BEFORE calling here, so a miss here
// means "no such live local endpoint" -> Python raises EndpointNotFoundError. Returns
// a _Connection on success or None on a miss.
py::object endpoint_connect_local(const std::string& peer_id) {
    auto seam = rayx_endpoint::EndpointRegistry::instance().resolve(peer_id);
    if (!seam) return py::none();
    return py::cast(_Connection(std::weak_ptr<rayx_endpoint::EndpointSeam>(seam)));
}

// Cross-process (same-node) connect over the A1 AF_UNIX transport. Probes reachability
// within `timeout_ms` and returns (status, conn): status 0 -> a remote _Connection;
// 5 -> timeout; 6 -> unreachable (nothing listening). Python maps 5/6 to typed errors.
py::object endpoint_connect_unix(const std::string& peer_id,
                                 const std::string& sock_path, long timeout_ms) {
    int status;
    {
        py::gil_scoped_release release;
        status = rayx_endpoint::transport_probe(sock_path, timeout_ms);
    }
    if (status == 0)
        return py::make_tuple(
            0, py::cast(_Connection(sock_path, peer_id, timeout_ms)));
    return py::make_tuple(status, py::none());
}

}  // namespace

PYBIND11_MODULE(_rayx, m) {
    m.doc() = "RayX rayx: minimal Python frontend over HPX service lanes";

    py::class_<EngineFuture>(m, "_Future")
        .def("result", &EngineFuture::result,
             "Block for the result; returns C++-measured timing fields.")
        .def("ready", &EngineFuture::ready,
             "Non-blocking: True if the result is ready. Raises if the future "
             "was already retired via result().")
        .def("cancelled", &EngineFuture::cancelled,
             "Non-blocking, non-consuming: True once this future's cancellation "
             "is settled (queued cancel or a running stop-at-boundary), even "
             "before the cancelled row is fulfilled. False for a non-cancelable "
             "(batch) future. Safe before and after retire.");

    py::class_<Engine>(m, "_Engine")
        .def(py::init<int, int, int, std::string>(), py::arg("num_lanes"),
             py::arg("hpx_threads"), py::arg("max_queue_depth_per_lane") = -1,
             py::arg("lane_impl") = "std")
        .def("submit", &Engine::submit, py::arg("service_ms"),
             py::arg("work_mode"), py::arg("chunks"), py::arg("chunk_delay_ms"))
        .def("submit_batch", &Engine::submit_batch, py::arg("service_ms"),
             py::arg("count"), py::arg("work_mode"))
        .def("submit_batch_varied", &Engine::submit_batch_varied,
             py::arg("service_ms_list"), py::arg("work_mode"),
             "Bulk submit one request per element of service_ms_list (each with "
             "its own service time), one Python->C++ crossing, same round-robin "
             "routing as submit_batch.")
        .def("_submit_batch_cost_probe", &Engine::submit_batch_cost_probe,
             py::arg("count"), py::arg("work_mode") = "sleep",
             "INTERNAL DIAGNOSTIC (not public API): attribute submit_batch cost "
             "into enqueue vs pybind-wrap vs drain over `count` no-op requests. "
             "Returns a dict of ns timings. Does not change submit_batch or any "
             "schema.")
        .def("_set_bulk_enqueue", &Engine::set_bulk_enqueue, py::arg("on"),
             "INTERNAL A/B toggle (not public API): true=per-lane bulk batch "
             "enqueue (default), false=one-by-one. Affects only batch enqueue "
             "strategy, not observable behavior/schema.")
        .def("wait", &Engine::wait, py::arg("futures"),
             py::arg("num_returns") = 1,
             "Block (GIL released) until >= num_returns of the given _Future "
             "objects are ready; return the indices of all ready ones. Does "
             "not consume the futures.")
        .def("cancel", &Engine::cancel, py::arg("future"),
             "Cancel a single submitted request: queued skip, or a running "
             "chunked request stops at its next chunk boundary. Returns True iff "
             "this call settles a cancellation; False if it already started a "
             "single-chunk request, is on its final chunk, or already "
             "completed/cancelled. Raises if retired, not cancelable, or the "
             "engine is shut down.")
        .def("num_lanes", &Engine::num_lanes)
        .def("lane_stats", &Engine::lane_stats,
             "Observability snapshot (debugging only): list of per-lane dicts "
             "{actor_id, queue_depth, active} in stable lane order. Non-consuming; "
             "snapshot can race; raises if the engine is shut down. Not scheduler "
             "state, not placement control, not part of the JSONL schema.")
        .def("shutdown", &Engine::shutdown);

    m.def("hpx_smoke", &hpx_smoke,
          "Start HPX as a library, run a trivial async, shut down cleanly; "
          "returns {'status': 'ok', 'value': 42}.");

    // ---- rayx.runtime runtime bindings ----------------------------------
    // Additive; surfaced in Python under `rayx.runtime`, NOT in rayx.__all__.

    py::class_<RuntimeFuture>(m, "_RuntimeFuture")
        .def("result", &RuntimeFuture::result,
             "Block (GIL released) for the operation result; returns a dict of "
             "the C++-measured row fields plus the operation value/has_value. "
             "Consume-once: a second call raises.")
        .def("ready", &RuntimeFuture::ready,
             "Non-blocking: True if the result is ready. Raises if already "
             "retired via result().")
        .def("cancel", &RuntimeFuture::cancel,
             "Cancel this operation (queued skip, or cooperative running stop at "
             "the next checkpoint). Runs on the calling thread with no HPX hop. "
             "Returns True iff this call settles a cancellation; False if already "
             "completed/cancelled or running a non-checkpointed op.")
        .def("cancelled", &RuntimeFuture::cancelled,
             "Non-consuming: True once cancellation is guaranteed; valid before "
             "and after result().");

    py::class_<RuntimeEngine>(m, "_RuntimeEngine")
        .def(py::init<int, int, int, bool, int>(), py::arg("hpx_threads"),
             py::arg("num_lanes"), py::arg("max_queue_depth_per_lane"),
             py::arg("nonblocking_op_lanes") = false,
             py::arg("max_inflight_per_lane") = 1)
        .def("submit_operation", &RuntimeEngine::submit_operation,
             py::arg("op_id"), py::arg("args"),
             "Dispatch one registered native operation through a round-robin "
             "RuntimeLane (HPX-native FIFO: the lane worker runs the op via "
             "hpx::async(exec_, ...).get()); returns a _RuntimeFuture.")
        .def("create_actor", &RuntimeEngine::create_actor,
             py::arg("actor_type"), py::arg("args"),
             "Create one local stateful native actor of a registered type over a "
             "dedicated HPX-native FIFO RuntimeLane; returns its opaque actor_id "
             "(rt-act- prefix). Native plumbing only (no Python ActorHandle / "
             "boundary validation yet).")
        .def("call_actor_method", &RuntimeEngine::call_actor_method,
             py::arg("actor_id"), py::arg("method_id"), py::arg("args"),
             "Dispatch one registered method on an existing actor onto its "
             "dedicated lane; returns a _RuntimeFuture (or None if a per-lane cap "
             "is set and the lane is full). Reuses the lane/cancel-token machinery.")
        .def("wait", &RuntimeEngine::wait, py::arg("futures"),
             py::arg("num_returns"),
             "Block (GIL released) via hpx::wait_some until at least num_returns "
             "of the given _RuntimeFuture objects are ready; return the indices of "
             "ALL currently-ready futures. Non-consuming (futures stay valid). "
             "Mirrors _Engine.wait.")
        .def("num_lanes", &RuntimeEngine::num_lanes,
             "Number of RuntimeLanes owned by this runtime.")
        .def("lane_stats", &RuntimeEngine::lane_stats,
             "Per-lane observability snapshot: a list of "
             "{actor_id, queue_depth, active} dicts in stable lane order. "
             "Non-consuming; raises if shut down. Not scheduler state, not "
             "placement control, not part of the JSONL schema.")
        .def("actor_stats", &RuntimeEngine::actor_stats, py::arg("actor_id"),
             "Per-actor observability snapshot (debugging only): one "
             "{actor_id, queue_depth, active} dict for the actor's dedicated "
             "lane. Non-consuming and racy; raises RuntimeError if shut down, "
             "ValueError on an unknown actor_id. lane_stats() stays "
             "op-lanes-only. Not scheduler state, not placement control, not "
             "part of any JSONL schema.")
        .def("barrier_fanin_witness", &RuntimeEngine::barrier_fanin_witness,
             "exp44 debug-only structural witness for the last barrier_fanin "
             "execution: {seq, observed_os_workers, leaves_requested, "
             "arrived_count, released_count, opener, reduction_after_all_leaves, "
             "ordering_violations, clean_exit, watchdog_opened, joined_count, "
             "max_simultaneously_suspended_leaves}. Mutex-guarded snapshot (no "
             "torn read; racy only means stale/cross-call); single-in-flight for "
             "tests/experiment. barrier_fanin is the one side-effecting registry "
             "op. Raises if shut down. Not scheduler state, not a synchronization "
             "primitive, not placement control, not a perf metric; "
             "max_simultaneously_suspended_leaves is coordinated suspension, not "
             "parallelism.")
        .def("overlap_witness", &RuntimeEngine::overlap_witness,
             "exp47 debug-only structural witness for the last overlap_probe "
             "execution: {seq, mode, observed_os_workers, arms_launched, "
             "arms_completed, max_in_flight, both_in_flight, per_arm_enter_seq, "
             "per_arm_leave_seq, per_arm_chunk_event_count, per_arm_worker_ids, "
             "per_arm_worker_ids_overflowed, ordering_violations, clean_exit, "
             "classification, event_count, event_trace_overflowed, events}. "
             "Mutex-guarded snapshot (no torn read; racy only means stale/cross-call); "
             "single-in-flight for tests/experiment. overlap_probe is the second "
             "side-effecting registry op. Raises if shut down. Not scheduler state, not "
             "a synchronization primitive, not placement control, not a perf metric; "
             "'in flight' is active within the bracketed arm compute (cooperative "
             "interleaving is not OS-thread parallelism), and 'worker_parallel' means "
             "only distinct workers observed.")
        .def("release_actor", &RuntimeEngine::release_actor,
             py::arg("actor_id"),
             "Release ONE local native actor: cancel its queued method calls, "
             "ask an in-flight checkpointed method to stop at its next "
             "boundary, drain its dedicated lane (every outstanding future "
             "still resolves), join the lane worker, then drop the native "
             "state. Synchronous and bounded by one checkpoint stride. Raises "
             "RuntimeError if the runtime is shut down, ValueError on an "
             "unknown actor_id. Local explicit release only -- not ray.kill, "
             "no distributed ownership, no refcounting.")
        .def("shutdown", &RuntimeEngine::shutdown);

    m.def("runtime_op_table", []() {
        // Typed signatures (closed value model: int64 / finite double):
        // {op_id: {"arg_types": [str, ...], "result_type": str}}. Merge the
        // HPX-free core registry (square/add/boom/busy_sum/scale_double) with the
        // HPX-side registry (fanout_sum/park_ms) so the Python boundary validates
        // every registered op id, its arity (== len(arg_types)), and each arg's
        // declared type + domain uniformly.
        py::dict d;
        auto add = [&d](const std::unordered_map<std::string,
                                                  rayx_runtime::OpEntry>& reg) {
            for (const auto& kv : reg) {
                py::list arg_types;
                for (rayx_runtime::OpType t : kv.second.arg_types)
                    arg_types.append(py::str(rayx_runtime::op_type_name(t)));
                py::dict sig;
                sig["arg_types"] = arg_types;
                sig["result_type"] =
                    py::str(rayx_runtime::op_type_name(kv.second.result_type));
                d[py::str(kv.first)] = sig;
            }
        };
        add(rayx_runtime::registry());
        add(rayx_runtime::hpx_registry());
        return d;
    }, "Return {op_id: {arg_types, result_type}} typed signatures for the fixed "
       "runtime operation registry (core + HPX-side composed ops; used by "
       "rayx.runtime for Python-boundary type validation).");

    m.def("runtime_actor_table", []() {
        // Typed metadata for the fixed actor registry (rayx_runtime::actor_registry):
        // {actor_type: {"init_arg_types": [str, ...],
        //               "methods": {method_id: {"arg_types": [str, ...],
        //                                        "result_type": str}}}}.
        // Mirrors runtime_op_table()'s shape so a future Python boundary can validate
        // actor create/call (unknown type/method, arity, per-arg type) -- NOT wired
        // into the rayx.runtime Python layer yet (C1b is native plumbing only).
        py::dict out;
        for (const auto& kv : rayx_runtime::actor_registry()) {
            const rayx_runtime::ActorTypeEntry& entry = kv.second;
            py::list init_types;
            for (rayx_runtime::OpType t : entry.init_arg_types)
                init_types.append(py::str(rayx_runtime::op_type_name(t)));
            py::dict methods;
            for (const auto& mkv : entry.methods) {
                py::list arg_types;
                for (rayx_runtime::OpType t : mkv.second.arg_types)
                    arg_types.append(py::str(rayx_runtime::op_type_name(t)));
                py::dict sig;
                sig["arg_types"] = arg_types;
                sig["result_type"] =
                    py::str(rayx_runtime::op_type_name(mkv.second.result_type));
                methods[py::str(mkv.first)] = sig;
            }
            py::dict type_entry;
            type_entry["init_arg_types"] = init_types;
            type_entry["methods"] = methods;
            out[py::str(kv.first)] = type_entry;
        }
        return out;
    }, "Return {actor_type: {init_arg_types, methods: {method_id: {arg_types, "
       "result_type}}}} typed metadata for the fixed runtime actor registry "
       "(counter add/get/reset). Not wired into the rayx.runtime Python layer yet.");

    // ---- experimental endpoint identity seam --------------------------------
    // Thin native surface; rayx.endpoint wraps these with validation + typed errors.
    // NOT a fabric / transport / socket; no performance claim.
    py::class_<_Connection>(m, "_EndpointConnection")
        .def("ping", &_Connection::ping, py::arg("nonce"),
             "Peer-specific typed int64 handshake. Same-process: computed INLINE against "
             "the registered seam. Cross-process: one plain-IPC (AF_UNIX) round-trip. NO "
             "HPX in either path. Returns (status, value): 0 ok / 1 peer-closed / 2 "
             "conn-closed / 3 not-found / 4 proto-err / 5 timeout.")
        .def("call_op", &_Connection::call_op, py::arg("op_code"), py::arg("args"),
             "EXPERIMENTAL endpoint->Runtime bridge (v1): call one fixed registered Runtime "
             "op (closed op-code enum) in the peer's process and return (status, value). "
             "Same-process dispatches in-process through the drain-gated bridge; cross-"
             "process is one AF_UNIX round trip. status is a CallStatus; value is int/float "
             "on CALL_OK else None. Plumbing only -- no HPX value/performance claim.")
        .def("close", &_Connection::close, "Mark this connection closed (idempotent).");

    py::class_<_Endpoint>(m, "_Endpoint")
        .def(py::init<bool>(), py::arg("transport") = false,
             "Mint a process-local endpoint identity (rtb-ep-<16 hex>) and register its "
             "seam. Variant 2: an endpoint is HPX-FREE -- it attaches to the shared HPX "
             "owner as ENDPOINT for policy/bookkeeping only and never starts HPX. With "
             "transport=True also join the process-local AF_UNIX listener (started lazily "
             "for the first transport endpoint) so cross-process pings can be delivered "
             "(A1 plain IPC, not HPX). Coexists with a Runtime and other Endpoints; raises "
             "only if an exclusive Engine is active.")
        .def("id", &_Endpoint::id)
        .def("pid", &_Endpoint::pid)
        .def("proto_version", &_Endpoint::proto_version)
        .def("closed", &_Endpoint::closed)
        .def("transport_kind", &_Endpoint::transport_kind,
             "'unix' if this endpoint joined the A1 transport listener, else 'none'.")
        .def("transport_addr", &_Endpoint::transport_addr,
             "The AF_UNIX socket path peers dial for cross-process delivery, or '' when "
             "transport is off.")
        .def("close", &_Endpoint::close,
             "Unregister the seam (tombstone), drop this endpoint's transport share "
             "(stopping the listener at the last transport endpoint), and detach the "
             "ENDPOINT policy slot. Variant 2: HPX-free -- this NEVER touches HPX. "
             "Idempotent.");

    m.def("_endpoint_connect_local", &endpoint_connect_local, py::arg("peer_id"),
          "Resolve peer_id in the PROCESS-LOCAL endpoint registry. Returns an "
          "_EndpointConnection, or None if no such live local endpoint. Cross-process "
          "peers go through _endpoint_connect_unix instead.");

    m.def("_endpoint_connect_unix", &endpoint_connect_unix, py::arg("peer_id"),
          py::arg("sock_path"), py::arg("timeout_ms"),
          "Cross-process (same-node) connect over the A1 AF_UNIX transport. Returns "
          "(status, conn): 0 -> _EndpointConnection, 5 -> timeout, 6 -> unreachable. "
          "Plain native local IPC; NO HPX.");

    m.def("_endpoint_shutdown", &endpoint_shutdown,
          "Idempotent endpoint cleanup helper (Variant 2): endpoints are HPX-free, so this "
          "NEVER stops HPX and NEVER raises. Normal cleanup is per-Endpoint.close(); this "
          "only clears tombstones / a stray listener when no endpoints remain.");
    m.def("_process_hpx_active", []() {
              return ProcessHpxOwner::instance().hpx_active();
          },
          "True iff the process HPX runtime is up (owned by a Runtime/Engine). Endpoints "
          "are HPX-free and never make this True. Debug/test gating only.");

    m.attr("_ENDPOINT_PROTO_VERSION") = rayx_endpoint::ENDPOINT_PROTO_VERSION;
    m.attr("_ENDPOINT_PING_XOR") =
        static_cast<std::int64_t>(rayx_endpoint::ENDPOINT_PING_XOR);

    // Install the endpoint->Runtime bridge (v1) CALL_OP handler so the transport accept
    // thread can perform drain-gated Runtime dispatch. Set once at module load; it consults
    // the RuntimeBridgeSlot per call, so it correctly reports "runtime unavailable" whenever
    // no Runtime is attached. NOT public API.
    rayx_endpoint::bridge_call_op_handler() = bridge_dispatch;

    // Bridge closed op-code enum (mirror _BRIDGE_OPS in rayx/endpoint/__init__.py).
    m.attr("_BRIDGE_OP_SQUARE") = static_cast<int>(rayx_endpoint::BRIDGE_OP_SQUARE);
    m.attr("_BRIDGE_OP_ADD") = static_cast<int>(rayx_endpoint::BRIDGE_OP_ADD);
    m.attr("_BRIDGE_OP_FANOUT_SUM") =
        static_cast<int>(rayx_endpoint::BRIDGE_OP_FANOUT_SUM);
    // Bridge v2 fanout_sum `n` cap (shutdown/drain bound only; not a perf knob).
    m.attr("_BRIDGE_FANOUT_N_MAX") =
        static_cast<std::int64_t>(rayx_endpoint::BRIDGE_FANOUT_N_MAX);
}
