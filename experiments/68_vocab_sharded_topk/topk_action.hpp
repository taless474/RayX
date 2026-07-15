// exp68 -- fixed registered HPX plain actions. Include in EXACTLY ONE TU PER BINARY
// (exp68_peer.cpp and actor_ext.cpp), so each binary registers the actions once -- the
// exp63/64/66/67 action-registration discipline. Closed, exactly-checkable values only.
#pragma once

#include <hpx/hpx.hpp>
#include <hpx/runtime_local/get_locality_id.hpp>

#include <unistd.h>  // getpid, gethostname

#include <cstdint>
#include <string>
#include <vector>

#include "shared_topk.hpp"

// Reply to a local-top-k request: the shard's local top-k candidates PLUS the executing process's
// identity witnesses (pid / locality / hostname), so the coordinator proves BY VALUE that the peer
// shard was computed on the peer's in-process HPX locality (not locally, not on the root).
struct exp68_topk_reply {
    std::vector<exp68::Cand> cands;
    std::int64_t pid = 0;
    std::uint32_t locality = 0xFFFFFFFFu;
    std::string host;

    template <typename Archive>
    void serialize(Archive& ar, unsigned) {
        // HPX intrusive serialization; std::vector<std::pair<int64,uint32>> + scalars are built-in.
        ar & cands & pid & locality & host;
    }
};

inline std::string exp68_this_host() {
    char buf[256] = {0};
    if (::gethostname(buf, sizeof(buf) - 1) == 0) return std::string(buf);
    return std::string("unknown");
}

// Compute the local top-k over half-open shard [lo, hi) with the deterministic rule + total order,
// and stamp the executing process/locality identity into the reply.
exp68_topk_reply exp68_local_topk(std::int64_t lo, std::int64_t hi, std::int64_t seed, std::int64_t k) {
    exp68_topk_reply r;
    r.cands = exp68::local_topk(lo, hi, seed, static_cast<int>(k));
    r.pid = static_cast<std::int64_t>(::getpid());
    r.locality = hpx::get_locality_id();
    r.host = exp68_this_host();
    return r;
}
HPX_PLAIN_ACTION(exp68_local_topk, exp68_local_topk_action)

// Identity probe: executing process pid, as int64 (compared against the Ray actor's os.getpid()).
std::int64_t exp68_pid() {
    return static_cast<std::int64_t>(::getpid());
}
HPX_PLAIN_ACTION(exp68_pid, exp68_pid_action)
