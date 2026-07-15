// exp66 -- fixed registered HPX plain actions. Include in EXACTLY ONE TU PER BINARY
// (exp66_peer.cpp and actor_ext.cpp), so each binary registers the actions once -- the exp63/64
// action-registration discipline. Closed int64 values only; no arbitrary payloads.
#pragma once

#include <hpx/hpx.hpp>
#include <hpx/runtime_local/get_locality_id.hpp>

#include <unistd.h>  // getpid

#include <cstdint>

#include "shared_probe.hpp"

// Closed-oracle probe: proves BY VALUE which locality executed the action.
std::int64_t exp66_probe(std::int64_t x) {
    return exp66::probe_value(x, hpx::get_locality_id());
}
HPX_PLAIN_ACTION(exp66_probe, exp66_probe_action)

// Identity probe: the executing PROCESS pid, as int64. Compared against the Ray actor's own
// os.getpid() -- the HPX-plane proof that the actor-hosted runtime serves in-process (no child).
std::int64_t exp66_pid() {
    return static_cast<std::int64_t>(::getpid());
}
HPX_PLAIN_ACTION(exp66_pid, exp66_pid_action)
