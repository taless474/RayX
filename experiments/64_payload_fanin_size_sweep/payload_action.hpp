// exp64 -- HPX payload leaf ACTION declaration + registration (EXPERIMENT-ONLY, Slice 1).
//
// COPIED/ADAPTED from exp63 collective_action.hpp (exp63 -> exp64) and EXTENDED with a RESPONSE
// PAYLOAD: the leaf record now carries S opaque bytes represented as an HPX
// `hpx::serialization::serialize_buffer<char>` -- the transport-facing payload type, NOT a naive
// std::vector<char> (which would benchmark the element-by-element serializer instead of the wire).
//
// exp64-SPECIFIC symbols so this can NEVER collide with exp62_leaf_action / exp63_leaf_action:
//   namespace  exp64
//   action     exp64_payload_leaf_action  (registered via HPX_PLAIN_ACTION here, ONE TU per binary)
//
// REGISTRATION DISCIPLINE (mirrors exp61/62/63):
//   * shared_payload.hpp stays PURE (oracle only, no HPX) and may be included anywhere.
//   * This header BOTH defines exp64_payload_leaf AND registers exp64_payload_leaf_action. Include it
//     in EXACTLY ONE TRANSLATION UNIT PER BINARY:
//        - payload_ext.cpp        (embedded-HPX root / Python module binary)
//        - payload_connector.cpp  (standalone connect-mode remote-locality binary)
//     Two SEPARATE binaries => the action registers exactly once per binary.
//
// The leaf runs ON the target locality, fills its OWN S payload bytes with payload_byte(x,i,k), and
// folds its OWN runtime locality id into the record. The digest is folded in PYTHON after timing --
// this header ships the raw bytes, it does NOT reduce them.
#pragma once

#include <cstddef>
#include <cstdint>
#include <utility>

#include <hpx/include/actions.hpp>
#include <hpx/include/serialization.hpp>
#include <hpx/serialization/serialize_buffer.hpp>
#include <hpx/runtime_local/get_locality_id.hpp>

#include "shared_payload.hpp"

namespace exp64 {

// Transport-facing payload type: HPX serialize_buffer participates in the parcelport's contiguous /
// zero-copy chunk handling, unlike a naive std::vector<char>.
using payload_buffer = hpx::serialization::serialize_buffer<char>;

// Serializable leaf record: scalar value + executing locality + S response payload bytes.
struct payload_leaf_record {
    std::int64_t value = 0;
    std::uint32_t locality = 0;
    payload_buffer payload;  // default: empty owning buffer

    template <typename Archive>
    void serialize(Archive& ar, unsigned const) {
        ar & value;
        ar & locality;
        ar & payload;  // serialize_buffer has its own HPX serialization
    }
};

}  // namespace exp64

// Free-function HPX plain action: closed-int64 leaf value + locality + S deterministic payload bytes.
// payload_bytes is the response payload size S; the leaf allocates an OWNING serialize_buffer<char> of
// size S and fills byte k with exp64::payload_byte(x, i, k).
inline exp64::payload_leaf_record exp64_payload_leaf(std::int64_t x, std::int64_t i,
                                                     std::int64_t payload_bytes) {
    exp64::payload_leaf_record r;
    r.value = exp64::leaf_value(x, i);
    r.locality = hpx::get_locality_id();
    const std::size_t s = payload_bytes > 0 ? static_cast<std::size_t>(payload_bytes) : 0;
    exp64::payload_buffer buf(s);  // owning allocation of S bytes
    for (std::size_t k = 0; k < s; ++k) {
        buf[k] = static_cast<char>(exp64::payload_byte(x, i, static_cast<std::int64_t>(k)));
    }
    r.payload = std::move(buf);
    return r;
}
HPX_PLAIN_ACTION(exp64_payload_leaf, exp64_payload_leaf_action)
