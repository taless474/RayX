// exp63 -- HPX leaf ACTION declaration + registration (EXPERIMENT-ONLY, Slice 1b).
//
// COPIED/RENAMED from exp62 fanout_action.hpp (exp62 -> exp63, exp62_leaf* -> exp63_leaf*). It is the
// ONE header that defines and registers the exp63 cross-locality leaf action, with exp63-SPECIFIC
// symbol/action names so it can NEVER collide with exp62's exp62_leaf_action. It includes NO exp62
// header.
//
// REGISTRATION DISCIPLINE (mirrors exp61/exp62's proven model):
//   * shared_collective.hpp stays PURE (oracle only, no HPX) and may be included anywhere.
//   * This header BOTH defines exp63_leaf AND registers exp63_leaf_action via HPX_PLAIN_ACTION. It
//     must be included in EXACTLY ONE TRANSLATION UNIT PER BINARY:
//        - collective_ext.cpp        (the embedded-HPX root / Python module binary)
//        - collective_connector.cpp  (the standalone connect-mode remote-locality binary)
//     Two SEPARATE binaries => the action is registered exactly once per binary. No other TU should
//     include this header.
//
// The leaf runs ON the target locality and folds its OWN runtime locality id into the record, so the
// witness is grounded in where the leaf ACTUALLY executed. The closed-int64 value proves intended
// execution; the locality id proves which locality ran it (node placement still needs hostnames /
// node ids / hard gates on the runner side).
#pragma once

#include <cstdint>

#include <hpx/include/actions.hpp>
#include <hpx/include/serialization.hpp>
#include <hpx/runtime_local/get_locality_id.hpp>

#include "shared_collective.hpp"

namespace exp63 {

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

// Serializable PARTIAL record for the Slice 2b depth-2 star-of-partials fan-in (NOT a k-ary tree).
// Each remote locality folds its OWN contiguous leaf block [i_begin, i_begin + i_count) locally (no
// further remote hop) and returns ONE partial, so the root composes r remote partial futures instead
// of N leaf futures. This is HAND-ROLLED on purpose rather than hpx::collectives::reduce: there is no
// fixed communicator and no generation/membership state, one action future per remote locality, and a
// failed/departed locality surfaces as an EXCEPTION through its future -- lower risk than a collective
// under connect-mode dynamic membership. i_begin/i_count carry the block so the root can prove the
// blocks tiled [0, n) exactly once. The closed-int64 partial_sum proves intended work; the locality id
// proves which locality folded it (node placement still needs hostnames / node ids / hard gates).
struct partial_record {
    std::int64_t partial_sum = 0;
    std::int64_t i_begin = 0;
    std::int64_t i_count = 0;
    std::uint32_t locality = 0;

    template <typename Archive>
    void serialize(Archive& ar, unsigned const) {
        ar & partial_sum;
        ar & i_begin;
        ar & i_count;
        ar & locality;
    }
};

}  // namespace exp63

// Free-function HPX plain action: closed-int64 leaf value + the executing locality id.
inline exp63::leaf_record exp63_leaf(std::int64_t x, std::int64_t i) {
    exp63::leaf_record r;
    r.value = exp63::leaf_value(x, i);
    r.locality = hpx::get_locality_id();
    return r;
}
HPX_PLAIN_ACTION(exp63_leaf, exp63_leaf_action)

// Free-function HPX plain action: local fold of leaf_value over the contiguous block
// [i_begin, i_begin + i_count), accumulated in uint64 (order-independent, mod 2^64) so it matches the
// Python oracle. Runs ON the target locality and folds its OWN block with NO further remote hop; the
// executing locality id is folded into the record. Same registration discipline as exp63_leaf_action:
// this header is included in EXACTLY ONE TU per binary (collective_ext.cpp, collective_connector.cpp),
// so exp63_partial_action is registered once per binary and the connector serves it too.
inline exp63::partial_record exp63_partial(std::int64_t x, std::int64_t i_begin, std::int64_t i_count) {
    exp63::partial_record r;
    std::uint64_t acc = 0;
    for (std::int64_t k = 0; k < i_count; ++k) {
        acc += static_cast<std::uint64_t>(exp63::leaf_value(x, i_begin + k));
    }
    r.partial_sum = static_cast<std::int64_t>(acc);
    r.i_begin = i_begin;
    r.i_count = i_count;
    r.locality = hpx::get_locality_id();
    return r;
}
HPX_PLAIN_ACTION(exp63_partial, exp63_partial_action)
