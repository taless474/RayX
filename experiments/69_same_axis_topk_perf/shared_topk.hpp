// exp69 -- pure deterministic top-k helpers shared by every exp69 binary/extension. NO HPX includes.
//
// EXACT COPY of the accepted exp68 workload semantics (experiments/68_vocab_sharded_topk/
// shared_topk.hpp), renamed into the exp69 namespace. exp69 is the SAME-AXIS performance
// experiment: it must run the IDENTICAL deterministic LLM-shaped synthetic workload (not real
// inference) through a Ray-mediated and an HPX-mediated peer-candidate path. Both arms call the
// SAME local_topk and merge_topk implementations below -- that shared-native-algorithm property is
// a gated same-axis invariant (native_compute_mismatch / native_merge_mismatch otherwise).
//
// Deterministic rule (MUST match run_exp69.py exactly):
//   h    = (uint32(token_id) * 2654435761u) + (uint32(seed) * 40503u)   [mod 2^32]
//   grid = int(h % 131u) - 65                                            [-65, 65]
//   logit= float32(grid) / 8.0f                                          [-8.125, 8.125], exact
//
// Total order (used by local top-k, both merges, AND the oracle):
//   1) higher logit wins; 2) on equal logit, lower global token id wins.
#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <utility>
#include <vector>

namespace exp69 {

// A candidate is a (global token id, float32 logit bit-pattern) pair -- serialization-safe and
// bit-exact. HPX serializes std::vector<std::pair<int64,uint32>> natively; the Ray arm carries the
// same pairs as Python (token_id, logit_bits) tuples (the approved transport contract).
using Cand = std::pair<std::int64_t, std::uint32_t>;

inline std::uint32_t f32_bits(float f) {
    std::uint32_t u;
    std::memcpy(&u, &f, sizeof(u));
    return u;
}

inline float bits_f32(std::uint32_t u) {
    float f;
    std::memcpy(&f, &u, sizeof(f));
    return f;
}

// Deterministic integer grid value in [-65, 65] for (token_id, seed).
inline int grid_value(std::int64_t token_id, std::int64_t seed) {
    std::uint32_t h = static_cast<std::uint32_t>(static_cast<std::uint32_t>(token_id) * 2654435761u)
                    + static_cast<std::uint32_t>(static_cast<std::uint32_t>(seed) * 40503u);
    return static_cast<int>(h % 131u) - 65;
}

// Exactly-reproducible float32 logit for (token_id, seed): grid / 8 (power-of-two, no rounding).
inline float logit_f32(std::int64_t token_id, std::int64_t seed) {
    return static_cast<float>(grid_value(token_id, seed)) / 8.0f;
}

inline std::uint32_t logit_bits(std::int64_t token_id, std::int64_t seed) {
    return f32_bits(logit_f32(token_id, seed));
}

// The single total order, expressed on candidates. Returns true iff a ranks strictly before b.
// No NaN/inf on the grid, so the float comparisons are well-defined.
inline bool better(const Cand& a, const Cand& b) {
    float la = bits_f32(a.second), lb = bits_f32(b.second);
    if (la != lb) return la > lb;        // higher logit wins
    return a.first < b.first;            // tie -> lower global token id wins
}

// Local top-k over the half-open shard [lo, hi): the k best tokens by the total order.
inline std::vector<Cand> local_topk(std::int64_t lo, std::int64_t hi, std::int64_t seed, int k) {
    std::vector<Cand> all;
    all.reserve(static_cast<std::size_t>(hi > lo ? hi - lo : 0));
    for (std::int64_t t = lo; t < hi; ++t) {
        all.emplace_back(t, logit_bits(t, seed));
    }
    std::stable_sort(all.begin(), all.end(), better);
    if (static_cast<int>(all.size()) > k) all.resize(static_cast<std::size_t>(k));
    return all;
}

// Merge two local candidate lists and take the global top-k by the same total order. Because each
// shard's local top-k of size k contains every global-top-k member from that shard, the union of
// both local top-k lists is a correct superset for the global top-k.
inline std::vector<Cand> merge_topk(std::vector<Cand> a, const std::vector<Cand>& b, int k) {
    a.insert(a.end(), b.begin(), b.end());
    std::stable_sort(a.begin(), a.end(), better);
    if (static_cast<int>(a.size()) > k) a.resize(static_cast<std::size_t>(k));
    return a;
}

}  // namespace exp69
