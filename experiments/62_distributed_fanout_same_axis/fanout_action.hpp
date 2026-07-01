// exp62 -- HPX leaf ACTION declaration + registration (EXPERIMENT-ONLY, Slice 2).
//
// This is the ONE header that defines and registers the cross-locality leaf action. It is the only
// place HPX_PLAIN_ACTION appears for exp62.
//
// REGISTRATION DISCIPLINE (mirrors exp61's proven model):
//   * shared_fanout.hpp stays PURE (oracle only, no HPX) and may be included anywhere.
//   * This header (fanout_action.hpp) BOTH defines exp62_leaf AND registers exp62_leaf_action via
//     HPX_PLAIN_ACTION. It must be included in EXACTLY ONE TRANSLATION UNIT PER BINARY:
//        - fanout_ext.cpp        (the embedded-HPX root / Python module binary)
//        - fanout_connector.cpp  (the standalone connect-mode remote-locality binary)
//     Two SEPARATE binaries => the action is registered exactly once per binary (no double
//     registration). No other TU should include this header.
//
// The leaf runs ON the target locality and folds its OWN runtime locality id into the record, so the
// Slice 0 witness is grounded in where the leaf ACTUALLY executed (not merely where we intended). The
// closed-int64 value proves intended execution; the locality id proves which locality ran it (node
// placement still needs hostnames / node ids / hard gates on the runner side).
#pragma once

#include <cstdint>

#include <hpx/include/actions.hpp>
#include <hpx/include/serialization.hpp>
#include <hpx/runtime_local/get_locality_id.hpp>

#include "shared_fanout.hpp"

namespace exp62 {

// Serializable leaf record returned by the action (value + the locality that executed the leaf).
struct leaf_record {
    std::int64_t value = 0;
    std::uint32_t locality = 0;

    template <typename Archive>
    void serialize(Archive& ar, unsigned const) {
        ar & value;
        ar & locality;
    }
};

}  // namespace exp62

// Free-function HPX plain action: closed-int64 leaf value + the executing locality id.
inline exp62::leaf_record exp62_leaf(std::int64_t x, std::int64_t i) {
    exp62::leaf_record r;
    r.value = exp62::leaf_value(x, i);
    r.locality = hpx::get_locality_id();
    return r;
}
HPX_PLAIN_ACTION(exp62_leaf, exp62_leaf_action)
