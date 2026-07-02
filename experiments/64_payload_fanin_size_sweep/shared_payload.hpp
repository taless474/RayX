// exp64 -- shared closed-int64 + payload-byte oracle (EXPERIMENT-ONLY).
//
// COPIED/ADAPTED from exp63 shared_collective.hpp (exp63 -> exp64) and EXTENDED with the payload-byte
// oracle. Self-contained under exp64; includes NO exp62/exp63 header. HEADER-ONLY, pure (no HPX), so
// it is safe to include in any TU with no double-registration trap.
//
// Mirrors the Slice 0 Python oracle in run_exp64_payload.py EXACTLY:
//     leaf_value(x, i)   = (x ^ 0x52415958) + (i << 1)                 (wrapped to int64)
//     composite_oracle   = sum of leaf_value(x, i), i in [0, n)        (mod 2^64, order-independent)
//     payload_byte(x,i,k)= low 8 bits of (uint64(leaf_value(x,i)) + k) (per-leaf sawtooth, period 256)
//
// The payload DIGEST is folded in PYTHON after timing (response bytes cross the Python boundary), so
// this header intentionally has NO digest fold -- the leaf only FILLS bytes with payload_byte. Oracle
// correctness proves intended closed-value execution; it does NOT, by itself, prove physical placement.
// NOT rayx.runtime, NOT _rayx, NO Ray.
#pragma once

#include <cstdint>

namespace exp64 {

constexpr std::int64_t LEAF_XOR = 0x52415958LL;  // "RAYX"

// Per-leaf closed scalar. uint64 arithmetic so the wrap is well-defined (signed overflow is UB).
inline std::int64_t leaf_value(std::int64_t x, std::int64_t i) {
    std::uint64_t v = (static_cast<std::uint64_t>(x) ^ static_cast<std::uint64_t>(LEAF_XOR))
                      + (static_cast<std::uint64_t>(i) << 1);
    return static_cast<std::int64_t>(v);
}

// Composite oracle: int64 sum over leaves, accumulated in uint64 (mod 2^64), order-independent.
inline std::int64_t composite_oracle(std::int64_t x, std::int64_t n) {
    std::uint64_t acc = 0;
    for (std::int64_t i = 0; i < n; ++i) {
        acc += (static_cast<std::uint64_t>(x) ^ static_cast<std::uint64_t>(LEAF_XOR))
               + (static_cast<std::uint64_t>(i) << 1);
    }
    return static_cast<std::int64_t>(acc);
}

// k-th payload byte of leaf i: low 8 bits of (uint64(leaf_value(x,i)) + k). Matches Slice 0 Python
// payload_byte. Used by the leaf to FILL its S-byte buffer; the digest is folded in Python.
inline unsigned char payload_byte(std::int64_t x, std::int64_t i, std::int64_t k) {
    std::uint64_t lv = static_cast<std::uint64_t>(leaf_value(x, i));
    return static_cast<unsigned char>((lv + static_cast<std::uint64_t>(k)) & 0xFFu);
}

}  // namespace exp64
