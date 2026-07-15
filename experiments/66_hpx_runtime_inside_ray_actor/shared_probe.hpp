// exp66 -- pure closed-oracle helpers shared by every exp66 binary/extension. NO HPX includes.
//
// Closed int64 oracle, exp65-style: probe_value(x, locality_id) is exactly reproducible in
// Python, so the controller verifies remote execution by VALUE and recovers the executing
// locality from the result. Synthetic; carries no inference, performance, or Ray-semantics claim.
#pragma once

#include <cstdint>

namespace exp66 {

inline constexpr std::int64_t kProbeXor = 0x66C0DE;

inline std::int64_t probe_value(std::int64_t x, std::uint32_t locality_id) {
    return (x ^ kProbeXor) + (static_cast<std::int64_t>(locality_id) << 1);
}

inline std::uint32_t executed_locality_from(std::int64_t result, std::int64_t x) {
    return static_cast<std::uint32_t>((result - (x ^ kProbeXor)) >> 1);
}

}  // namespace exp66
