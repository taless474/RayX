// exp69 -- fixed registered HPX plain actions. Include in EXACTLY ONE TU PER BINARY
// (exp69_peer.cpp and actor_ext.cpp), so each binary registers the actions once -- the
// exp63/64/66/67/68 action-registration discipline. Closed, exactly-checkable values only.
#pragma once

#include <hpx/hpx.hpp>
#include <hpx/runtime_local/get_locality_id.hpp>

#include <unistd.h>  // getpid, gethostname

#include <atomic>
#include <chrono>    // Slice 3 diagnostic: peer-LOCAL monotonic timing (never subtracted cross-node)
#include <cstdint>
#include <string>
#include <vector>

#include "shared_topk.hpp"

// MECHANISM WITNESS: monotonic count of exp69 top-k actions SERVED BY THIS PROCESS. The controller
// snapshots it (via a Ray meta method, outside every timed window) to prove per request that the
// HPX arm used exactly one HPX action and the Ray arm used none (hpx_action_used_by_ray_arm /
// hpx_action_witness_missing otherwise). Diagnostic witness only -- not scheduler state, not a
// synchronization primitive, no performance meaning.
inline std::atomic<long long> exp69_action_serve_count{0};

// ACTIVE-CALL WITNESS (Slice 2 throughput): count of exp69 peer HPX top-k actions CONCURRENTLY
// EXECUTING in this process. `current` is incremented on action entry and decremented on every exit
// path (RAII), `max` is the running peak since the last between-batch reset. Snapshot-safe atomics;
// proves genuine peer-action service overlap (>=2) under bounded concurrency. NOT scheduler state,
// not a synchronization primitive, no performance meaning; unread and inert under QD1 (Slice 1).
inline std::atomic<long long> exp69_hpx_action_active_current{0};
inline std::atomic<long long> exp69_hpx_action_active_max{0};

struct exp69_hpx_action_active_guard {
    exp69_hpx_action_active_guard() {
        long long cur = exp69_hpx_action_active_current.fetch_add(1, std::memory_order_relaxed) + 1;
        long long prev = exp69_hpx_action_active_max.load(std::memory_order_relaxed);
        while (cur > prev && !exp69_hpx_action_active_max.compare_exchange_weak(
                                 prev, cur, std::memory_order_relaxed)) {
        }
    }
    ~exp69_hpx_action_active_guard() {
        exp69_hpx_action_active_current.fetch_sub(1, std::memory_order_relaxed);
    }
};

// Reset the active-call MAX to the current in-flight (0 between batches). Call ONLY between batches,
// never during a measured window (the max must not be reset mid-repetition).
inline void exp69_reset_hpx_action_active() {
    exp69_hpx_action_active_max.store(
        exp69_hpx_action_active_current.load(std::memory_order_relaxed), std::memory_order_relaxed);
}

// Reply to a local-top-k request: the shard's local top-k candidates PLUS the executing process's
// identity witnesses (pid / locality / hostname), so the coordinator proves BY VALUE that the peer
// shard was computed on the peer's in-process HPX locality (not locally, not on the root).
struct exp69_topk_reply {
    std::vector<exp69::Cand> cands;
    std::int64_t pid = 0;
    std::uint32_t locality = 0xFFFFFFFFu;
    std::string host;

    template <typename Archive>
    void serialize(Archive& ar, unsigned) {
        // HPX intrusive serialization; std::vector<std::pair<int64,uint32>> + scalars are built-in.
        ar & cands & pid & locality & host;
    }
};

inline std::string exp69_this_host() {
    char buf[256] = {0};
    if (::gethostname(buf, sizeof(buf) - 1) == 0) return std::string(buf);
    return std::string("unknown");
}

// Compute the local top-k over half-open shard [lo, hi) with the deterministic rule + total order,
// and stamp the executing process/locality identity into the reply. Increments the serve-count
// witness FIRST so a bounded-wait abandonment can never undercount a served action.
exp69_topk_reply exp69_local_topk(std::int64_t lo, std::int64_t hi, std::int64_t seed, std::int64_t k) {
    exp69_hpx_action_active_guard exp69_active_guard;  // Slice 2 concurrent-execution witness (RAII)
    exp69_action_serve_count.fetch_add(1, std::memory_order_relaxed);
    exp69_topk_reply r;
    r.cands = exp69::local_topk(lo, hi, seed, static_cast<int>(k));
    r.pid = static_cast<std::int64_t>(::getpid());
    r.locality = hpx::get_locality_id();
    r.host = exp69_this_host();
    return r;
}
HPX_PLAIN_ACTION(exp69_local_topk, exp69_local_topk_action)

// ---- Slice 3 (exp69) DIAGNOSTIC-ONLY timed peer action ------------------------------------------
// SAME closed workload and SAME witnesses as exp69_local_topk (RAII active guard + serve-count
// increment), so Slice-3 diagnostic batches reuse the accepted Slice-2 witness gates UNCHANGED.
// It additionally stamps PEER-LOCAL monotonic durations (measured entirely on the peer's own
// steady_clock) into an extended reply. These peer-local timings are for peer-side attribution ONLY
// and MUST NOT be subtracted from the coordinator's clock. The accepted exp69_local_topk_action is
// left byte-identical; this is a separate registered action used only by coordinate_diag_timed.
// Diagnostic witness only -- not scheduler state, no performance claim.
struct exp69_topk_reply_timed {
    std::vector<exp69::Cand> cands;
    std::int64_t pid = 0;
    std::uint32_t locality = 0xFFFFFFFFu;
    std::string host;
    long long peer_local_topk_ns = -1;    // peer-local: local_topk sort duration
    long long peer_action_body_ns = -1;   // peer-local: whole action-body duration

    template <typename Archive>
    void serialize(Archive& ar, unsigned) {
        ar & cands & pid & locality & host & peer_local_topk_ns & peer_action_body_ns;
    }
};

exp69_topk_reply_timed exp69_local_topk_timed(std::int64_t lo, std::int64_t hi, std::int64_t seed,
                                              std::int64_t k) {
    auto body0 = std::chrono::steady_clock::now();
    exp69_hpx_action_active_guard exp69_active_guard;  // SAME concurrent-execution witness (RAII)
    exp69_action_serve_count.fetch_add(1, std::memory_order_relaxed);  // SAME serve-count witness
    exp69_topk_reply_timed r;
    auto s0 = std::chrono::steady_clock::now();
    r.cands = exp69::local_topk(lo, hi, seed, static_cast<int>(k));
    auto s1 = std::chrono::steady_clock::now();
    r.pid = static_cast<std::int64_t>(::getpid());
    r.locality = hpx::get_locality_id();
    r.host = exp69_this_host();
    r.peer_local_topk_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(s1 - s0).count();
    r.peer_action_body_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                std::chrono::steady_clock::now() - body0)
                                .count();
    return r;
}
HPX_PLAIN_ACTION(exp69_local_topk_timed, exp69_local_topk_timed_action)

// Identity probe: executing process pid, as int64 (compared against the Ray actor's os.getpid()).
std::int64_t exp69_pid() {
    return static_cast<std::int64_t>(::getpid());
}
HPX_PLAIN_ACTION(exp69_pid, exp69_pid_action)
