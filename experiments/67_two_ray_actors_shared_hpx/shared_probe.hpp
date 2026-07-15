// exp67 -- pure closed-oracle helpers shared by every exp67 binary/extension. NO HPX includes.
//
// Closed int64 oracle (exp65/exp66 lineage): probe_value(x, locality_id) is exactly reproducible
// in Python, so the controller verifies remote execution BY VALUE and recovers the executing
// locality from the result. exp67 uses a distinct XOR constant so an exp67 result can never be
// confused with an exp66 one. Synthetic; no inference, performance, or Ray-semantics claim.
#pragma once

#include <cstdint>

namespace exp67 {

inline constexpr std::int64_t kProbeXor = 0x67C0DE;

inline std::int64_t probe_value(std::int64_t x, std::uint32_t locality_id) {
    return (x ^ kProbeXor) + (static_cast<std::int64_t>(locality_id) << 1);
}

inline std::uint32_t executed_locality_from(std::int64_t result, std::int64_t x) {
    return static_cast<std::uint32_t>((result - (x ^ kProbeXor)) >> 1);
}

}  // namespace exp67
