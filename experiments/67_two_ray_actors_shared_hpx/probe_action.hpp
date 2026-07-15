// exp67 -- fixed registered HPX plain actions. Include in EXACTLY ONE TU PER BINARY
// (exp67_peer.cpp and actor_ext.cpp), so each binary registers the actions once -- the
// exp63/64/66 action-registration discipline. Closed int64 values + a hostname provenance
// witness; no arbitrary payloads.
#pragma once

#include <hpx/hpx.hpp>
#include <hpx/runtime_local/get_locality_id.hpp>

#include <unistd.h>  // getpid, gethostname

#include <cstdint>
#include <string>

#include "shared_probe.hpp"

// Closed-oracle probe: proves BY VALUE which locality executed the action.
std::int64_t exp67_probe(std::int64_t x) {
    return exp67::probe_value(x, hpx::get_locality_id());
}
HPX_PLAIN_ACTION(exp67_probe, exp67_probe_action)

// Identity probe: the executing PROCESS pid, as int64. Compared against a Ray actor's own
// os.getpid() -- the HPX-plane proof that an actor-hosted runtime serves in-process (no child).
std::int64_t exp67_pid() {
    return static_cast<std::int64_t>(::getpid());
}
HPX_PLAIN_ACTION(exp67_pid, exp67_pid_action)

// Hostname provenance witness: the executing process's hostname, returned over the HPX plane so
// the "executing hostname" witness is genuinely remote-reported (not inferred by the controller).
std::string exp67_host() {
    char buf[256] = {0};
    if (::gethostname(buf, sizeof(buf) - 1) == 0) return std::string(buf);
    return std::string("unknown");
}
HPX_PLAIN_ACTION(exp67_host, exp67_host_action)
