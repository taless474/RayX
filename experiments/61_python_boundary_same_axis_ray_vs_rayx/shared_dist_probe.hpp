// exp61 -- shared closed-int64 dist_probe action + oracle for the same-axis Python-boundary
// experiment (EXPERIMENT-ONLY).
//
// The action keeps the SAME closed-int64 contract as exp58's `dist_probe`:
//     result = (x ^ 0x52415958) + (locality_id << 1)
// so a future Slice-1 connector built from this same header can interoperate over the
// parcelport. This is NOT rayx.runtime, NOT the _rayx extension, and has NO Ray anywhere.
//
// node_tag is the executing HPX locality id (0 for the single-locality Slice-0 smoke). Oracle
// correctness proves the intended locality executed and returned the expected value -- it does
// NOT, by itself, prove physical node placement.
//
// INCLUDE IN EXACTLY ONE TRANSLATION UNIT PER HPX BINARY (Slice 0: dist_probe_ext.cpp only):
// it both defines the action impl and registers it via HPX_PLAIN_ACTION, so a second includer
// in the same binary would be a (loud) double-registration link error.
#pragma once

#include <cstdint>

#include <hpx/include/actions.hpp>
#include <hpx/runtime_local/get_locality_id.hpp>

namespace exp61 {

constexpr std::int64_t DIST_PROBE_XOR = 0x52415958LL;  // "RAYX"

// Closed-int64 oracle, correctness-only.
inline std::int64_t dist_probe_oracle(std::int64_t x, std::uint32_t node_tag) {
    return (x ^ DIST_PROBE_XOR) + (static_cast<std::int64_t>(node_tag) << 1);
}

}  // namespace exp61

// Free-function HPX plain action (same shape as exp58's `dist_probe`): the locality that runs it
// folds its own id into the closed-int64 result.
inline std::int64_t exp61_dist_probe(std::int64_t x) {
    std::uint32_t loc = hpx::get_locality_id();
    return (x ^ exp61::DIST_PROBE_XOR) + (static_cast<std::int64_t>(loc) << 1);
}
HPX_PLAIN_ACTION(exp61_dist_probe, exp61_dist_probe_action)
