// exp62 -- shared closed-int64 fanout/fanin oracle (EXPERIMENT-ONLY).
//
// HEADER-ONLY oracle that mirrors the Slice 0 Python oracle in run_exp62_fanout.py:
//     leaf_value(x, i) = (x ^ 0x52415958) + (i << 1)            (wrapped to int64)
//     composite_oracle(x, n) = sum of leaf_value(x, i), i in [0, n)   (mod 2^64, order-independent)
//
// SLICE 1 DISCIPLINE: this header defines NO HPX action. There is NO HPX_PLAIN_ACTION here, so it is
// safe to include in any translation unit with no double-registration trap. Cross-locality action
// registration is DEFERRED to Slice 2's standalone connector binary, which is the only place a
// parcelport-dispatched action is needed.
//
// Oracle correctness proves the intended closed-int64 work executed and reduced -- it does NOT, by
// itself, prove physical placement. This is NOT rayx.runtime, NOT the _rayx extension, and has NO
// Ray anywhere.
#pragma once

#include <cstdint>

namespace exp62 {

constexpr std::int64_t LEAF_XOR = 0x52415958LL;  // "RAYX"

// Per-leaf oracle. Computed via uint64 so the wrap is well-defined (signed overflow would be UB).
inline std::int64_t leaf_value(std::int64_t x, std::int64_t i) {
    std::uint64_t v = (static_cast<std::uint64_t>(x) ^ static_cast<std::uint64_t>(LEAF_XOR))
                      + (static_cast<std::uint64_t>(i) << 1);
    return static_cast<std::int64_t>(v);
}

// Composite oracle: int64 sum over leaves, accumulated in uint64 (mod 2^64) so the result is
// order-independent and matches the Slice 0 Python i64(sum(...)).
inline std::int64_t composite_oracle(std::int64_t x, std::int64_t n) {
    std::uint64_t acc = 0;
    for (std::int64_t i = 0; i < n; ++i) {
        acc += (static_cast<std::uint64_t>(x) ^ static_cast<std::uint64_t>(LEAF_XOR))
               + (static_cast<std::uint64_t>(i) << 1);
    }
    return static_cast<std::int64_t>(acc);
}

}  // namespace exp62
