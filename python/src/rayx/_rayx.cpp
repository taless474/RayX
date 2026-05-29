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

#include "service_lane.hpp"

#include <hpx/hpx.hpp>
#include <hpx/hpx_start.hpp>

#include <atomic>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace py = pybind11;

namespace {

// ---- _Future ------------------------------------------------------------

// Move-only wrapper around one in-flight result future.
class EngineFuture {
public:
    explicit EngineFuture(hpx::future<rayhpx::Result> fut)
        : fut_(std::move(fut)) {}

    EngineFuture(EngineFuture&&) = default;
    EngineFuture& operator=(EngineFuture&&) = default;
    EngineFuture(const EngineFuture&) = delete;
    EngineFuture& operator=(const EngineFuture&) = delete;

    // Block (GIL released) for the result; return C++-measured fields only.
    // The Python layer adds submit_ns/total_ms/queue_wait_ms (its own clock).
    py::dict result() {
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

    // --- borrow/return helpers for Engine::wait (no consumption) -------------
    // wait() moves the underlying future out (take), waits on a temp vector,
    // then moves it back (put), so the Python _Future object -- and its
    // Python-side submit_ns mapping -- is preserved.
    bool valid_now() const { return fut_.valid(); }
    hpx::future<rayhpx::Result> take() { return std::move(fut_); }
    void put(hpx::future<rayhpx::Result> f) { fut_ = std::move(f); }

private:
    hpx::future<rayhpx::Result> fut_;
};

// ---- _Engine ------------------------------------------------------------

std::vector<char> to_cstr(const std::string& s) {
    std::vector<char> v(s.begin(), s.end());
    v.push_back('\0');
    return v;
}

class Engine {
public:
    Engine(int num_lanes, int hpx_threads) {
        if (num_lanes < 1)
            throw std::invalid_argument("num_lanes must be >= 1");
        if (hpx_threads < 1)
            throw std::invalid_argument("hpx_threads must be >= 1");

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

        lanes_.reserve(static_cast<std::size_t>(num_lanes));
        for (int i = 0; i < num_lanes; ++i) {
            lanes_.push_back(std::make_unique<rayhpx::ServiceLane>());
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

    EngineFuture submit(double service_ms, const std::string& work_mode) {
        if (!running_) throw std::runtime_error("Engine is shut down");
        rayhpx::Request req;
        req.service_ms_requested = service_ms;
        req.work_mode = work_mode;
        req.submit_ns = rayhpx::now_ns();
        rayhpx::ServiceLane& lane = *lanes_[rr_ % lanes_.size()];
        ++rr_;
        return EngineFuture(lane.submit(std::move(req)));
    }

    // Bulk submit: cross into C++ once, enqueue `count` requests using the same
    // round-robin lane routing as submit(), and return one _Future per request.
    // Returns a py::list (built here) because EngineFuture is move-only, so the
    // futures are cast out with return_value_policy::move rather than copied.
    py::list submit_batch(double service_ms, int count,
                          const std::string& work_mode) {
        if (!running_) throw std::runtime_error("Engine is shut down");
        if (count < 1) throw std::invalid_argument("count must be >= 1");
        py::list out;
        for (int i = 0; i < count; ++i) {
            rayhpx::Request req;
            req.service_ms_requested = service_ms;
            req.work_mode = work_mode;
            req.submit_ns = rayhpx::now_ns();
            rayhpx::ServiceLane& lane = *lanes_[rr_ % lanes_.size()];
            ++rr_;
            EngineFuture f(lane.submit(std::move(req)));
            out.append(py::cast(std::move(f), py::return_value_policy::move));
        }
        return out;
    }

    // As-completed wait. Block (GIL released) until at least num_returns of the
    // given _Future objects are ready, then return the indices (into the input
    // list) of ALL currently-ready futures. The caller retires the ones it
    // wants and keeps the rest in flight -- this is the primitive behind the
    // batch_wait retire loop and mirrors ray.wait(num_returns=k) / the native
    // hpx::wait_some. num_returns=1 gives wait_any semantics.
    //
    // It does NOT consume any future: the underlying hpx::futures are moved
    // into a temp vector for hpx::wait_some and moved back into the SAME
    // _Future objects (Python keeps ownership; each Future's Python-side
    // submit_ns is preserved). The move-back is exception-safe (RAII), and the
    // wait blocks inside HPX -- never a busy-poll under the GIL.
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
        // raises a clear error.
        std::vector<EngineFuture*> efs;
        efs.reserve(n);
        for (py::handle h : futures) {
            EngineFuture* ef = h.cast<EngineFuture*>();
            if (!ef->valid_now()) {
                throw std::runtime_error(
                    "wait() received a Future already retired via result()");
            }
            efs.push_back(ef);
        }

        // Move the futures out, and guarantee they are moved back even if
        // wait_some throws, so the Python _Future objects stay intact.
        std::vector<hpx::future<rayhpx::Result>> tmp;
        tmp.reserve(n);
        for (EngineFuture* ef : efs) tmp.push_back(ef->take());
        struct Restore {
            std::vector<EngineFuture*>& efs;
            std::vector<hpx::future<rayhpx::Result>>& tmp;
            ~Restore() {
                for (std::size_t i = 0; i < efs.size(); ++i)
                    efs[i]->put(std::move(tmp[i]));
            }
        } restore{efs, tmp};

        {
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

    std::vector<std::unique_ptr<rayhpx::ServiceLane>> lanes_;
    std::size_t rr_ = 0;
    bool running_ = false;
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
    m.doc() = "RayHPX rayx: minimal Python frontend over HPX service lanes";

    py::class_<EngineFuture>(m, "_Future")
        .def("result", &EngineFuture::result,
             "Block for the result; returns C++-measured timing fields.")
        .def("ready", &EngineFuture::ready,
             "Non-blocking: True if the result is ready. Raises if the future "
             "was already retired via result().");

    py::class_<Engine>(m, "_Engine")
        .def(py::init<int, int>(), py::arg("num_lanes"), py::arg("hpx_threads"))
        .def("submit", &Engine::submit, py::arg("service_ms"),
             py::arg("work_mode"))
        .def("submit_batch", &Engine::submit_batch, py::arg("service_ms"),
             py::arg("count"), py::arg("work_mode"))
        .def("wait", &Engine::wait, py::arg("futures"),
             py::arg("num_returns") = 1,
             "Block (GIL released) until >= num_returns of the given _Future "
             "objects are ready; return the indices of all ready ones. Does "
             "not consume the futures.")
        .def("num_lanes", &Engine::num_lanes)
        .def("shutdown", &Engine::shutdown);

    m.def("hpx_smoke", &hpx_smoke,
          "Start HPX as a library, run a trivial async, shut down cleanly; "
          "returns {'status': 'ok', 'value': 42}.");
}
