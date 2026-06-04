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

#include <hpx/hpx.hpp>
#include <hpx/hpx_start.hpp>

#include <atomic>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <unordered_set>
#include <vector>

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

        bool expected = false;
        if (!active().compare_exchange_strong(expected, true)) {
            throw std::runtime_error(
                "an Engine is already active in this process; call shutdown() "
                "on it before creating another");
        }

        try {
            start_hpx(hpx_threads);
        } catch (...) {
            active() = false;
            throw;
        }

        // Construct the lanes AFTER start_hpx: the HpxLane adapter ctor hops onto
        // an HPX thread to spawn the lane's worker, so the runtime must be up.
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
        // Join lane threads first (drains queued requests), then stop HPX.
        lanes_.clear();
        {
            py::gil_scoped_release release;
            hpx::post([]() { hpx::finalize(); });
            hpx::stop();
        }
        active() = false;
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

    static std::atomic<bool>& active() {
        static std::atomic<bool> a{false};
        return a;
    }

    static void start_hpx(int hpx_threads) {
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

    std::vector<std::unique_ptr<RayxLaneIface>> lanes_;
    std::size_t rr_ = 0;
    // Per-lane bounded-admission cap: -1 = unbounded (default); >= 1 = max
    // queued-but-not-started requests admitted per lane (see submit()).
    int max_qd_per_lane_ = -1;
    bool running_ = false;
    // Batch enqueue strategy (A/B; see set_bulk_enqueue). Default = bulk.
    bool use_bulk_enqueue_ = true;
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
}
