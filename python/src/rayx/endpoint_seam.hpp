// rayx endpoint seam, Slice 0 (+0.1 hardening): Ray bootstrap + HPX endpoint identity.
//
// EXPERIMENTAL and deliberately NARROW. This header is the HPX-FREE CORE of the
// `rayx.endpoint` seam: endpoint identity, a PROCESS-LOCAL registry, and the fixed
// (now peer-specific) transform. It is NOT a fabric, NOT a transport, NOT a socket,
// NOT parcelport, NOT AGAS, and carries NO performance claim.
//
// What Slice 0 PROVES (exactly this): endpoint identity, Ray bootstrap metadata
// exchange, local registry resolution, typed failure, and lifecycle. What it does
// NOT prove: HPX transport, HPX fabric, cross-process native delivery, or HPX
// performance.
//
//   * Each `Endpoint` mints a stable opaque identity `rtb-ep-<16 hex>` and registers a
//     seam in a process-local registry.
//   * In ONE process, endpoint A can resolve endpoint B's id in that registry and run
//     a fixed typed int64 ping/handshake against B's seam. The transform `on_ping`
//     below is a PURE, HPX-FREE function; the `_rayx.cpp` binding computes it INLINE on
//     the calling thread -- there is NO `hpx::run_as_hpx_thread` and NO HPX call in the
//     ping data path (local or cross-process).
//   * CROSS-PROCESS delivery is provided by the A1 transport slice as PLAIN NATIVE LOCAL
//     IPC (AF_UNIX) in `endpoint_transport.hpp`: a process-local listener resolves the
//     `target_endpoint_id` carried on the wire against THIS registry and computes the
//     same `on_ping` inline. HPX is NOT the delivery mechanism, NOT serving the socket,
//     and NOT computing the ping. See docs/design/endpoint_transport_slice.md. This
//     header stays HPX-free and transport-free: identity, registry, the pure transform,
//     and the fixed-size wire codec only (no sockets here).
//
// REGISTRY HONESTY: the process-local registry below is a STRUCTURAL PLACEHOLDER. It
// is NOT AGAS, NOT a fabric, NOT an addressing system, NOT an HPX transport. The A1
// AF_UNIX path is plain local IPC, not an HPX-managed channel (that is the later A2
// HPX-served work); later parcelport / AGAS work is NOT constrained by and must NOT
// preserve this registry's shape.
//
// Variant 2 lifecycle: an Endpoint is HPX-FREE. This header neither starts nor stops HPX,
// and the registry/seam below are plain mutex-guarded native state (no HPX). Endpoints
// attach to the shared process-level HPX owner (ProcessHpxOwner in _rayx.cpp) as ENDPOINT
// for POLICY/bookkeeping only -- they coexist with a Runtime (which owns HPX) and other
// Endpoints, but not with an exclusive Engine; they never bring HPX up. The transport
// listener refcount (transport=True) also lives in _rayx.cpp and is independent of HPX.

#ifndef RAYX_ENDPOINT_SEAM_HPP
#define RAYX_ENDPOINT_SEAM_HPP

#include "runtime_ops.hpp"  // rayx_runtime::make_runtime_actor_id (HPX-free id gen)

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace rayx_endpoint {

// Wire/identity contract constants. Mirror of ENDPOINT_PROTO_VERSION in
// rayx/endpoint/_validate.py and the ping transform documented in the integration
// test -- keep them in sync.
inline constexpr int ENDPOINT_PROTO_VERSION = 2;  // A1 six-key metadata + wire codec
inline constexpr const char* ENDPOINT_ID_PREFIX = "rtb-ep-";  // + 16 lowercase hex

// Base constant of the handshake transform. "RAYX" in ASCII. The full transform also
// mixes a hash of the PEER endpoint id (see on_ping) so the response is peer-specific.
inline constexpr std::int64_t ENDPOINT_PING_XOR = 0x52415958;

// FNV-1a 64-bit over the endpoint-id bytes, returned as a SIGNED int64 (a plain
// reinterpretation of the 64-bit pattern). Mirror of endpoint_id_hash() in
// rayx/endpoint/_validate.py -- keep the constants and the algorithm in sync. This is
// a structural per-endpoint mixin, NOT a cryptographic hash and NOT security.
inline std::int64_t endpoint_id_hash(const std::string& id) {
    std::uint64_t h = 0xcbf29ce484222325ULL;          // FNV offset basis
    for (unsigned char c : id) {
        h ^= static_cast<std::uint64_t>(c);
        h *= 0x100000001b3ULL;                          // FNV prime
    }
    return static_cast<std::int64_t>(h);               // reinterpret to signed int64
}

// Ping status codes returned to Python (mapped to typed errors there). The local
// (same-process registry) path uses 0/1/2; the cross-process transport path adds
// NOT_FOUND/PROTO_ERR/TIMEOUT (see endpoint_transport.hpp and endpoint/__init__.py).
enum PingStatus : int {
    PING_OK = 0,            // -> value
    PING_PEER_CLOSED = 1,   // -> EndpointClosedError (peer seam closed)
    PING_CONN_CLOSED = 2,   // -> EndpointClosedError (connection closed / peer gone)
    PING_NOT_FOUND = 3,     // -> EndpointNotFoundError (listener up, id not registered)
    PING_PROTO_ERR = 4,     // -> EndpointProtocolError (wire proto mismatch)
    PING_TIMEOUT = 5        // -> EndpointTimeoutError (deadline exceeded)
};

// ===== Endpoint->Runtime bridge (v1) =======================================
// EXPERIMENTAL, NARROW. The bridge lets a cross-process endpoint request trigger ONE fixed
// registered Runtime operation in the server process and return a typed result. It is
// PLUMBING ONLY: it proves the seam, NOT HPX value. See docs/design/endpoint_runtime_bridge.md.
// Closed op-code enum (no arbitrary op-id strings on the wire) -> mapped to a registered
// Runtime op-id by the bridge dispatch. v1 op codes use int64 args + int64 results only.
enum BridgeOpCode : int {
    BRIDGE_OP_SQUARE = 1,    // square(int64) -> int64
    BRIDGE_OP_ADD = 2,       // add(int64, int64) -> int64
    BRIDGE_OP_FANOUT_SUM = 3  // fanout_sum(int64 n, int64 parts) -> int64 (composed-op v2)
};

// Bridge v2 composed-op bound: the bridge caps fanout_sum's `n` so the (non-cancellable,
// serial-listener) in-flight op cannot stretch the shutdown drain wait. This is a SHUTDOWN/
// DRAIN BOUND ONLY -- not a performance knob, not a benchmark parameter, and nothing asserts
// timing. `n > BRIDGE_FANOUT_N_MAX` is rejected as CALL_OP_BADARGS before dispatch; n < 0 and
// parts range are left to the registered op body (-> CALL_OP_FAILED), reusing its semantics.
inline constexpr std::int64_t BRIDGE_FANOUT_N_MAX = 1000000;  // 1e6: conservative, bounded

// Bridge CALL_OP status codes. Shared by the wire response byte AND the Python-facing
// _Connection.call_op return (mirror _CALL_* in rayx/endpoint/__init__.py). The first six
// mirror the ping taxonomy; 6-11 are bridge/Runtime-specific.
enum CallStatus : int {
    CALL_OK = 0,                // -> value
    CALL_PEER_CLOSED = 1,       // -> EndpointClosedError
    CALL_CONN_CLOSED = 2,       // -> EndpointClosedError
    CALL_NOT_FOUND = 3,         // -> EndpointNotFoundError
    CALL_PROTO_ERR = 4,         // -> EndpointProtocolError
    CALL_TIMEOUT = 5,           // -> EndpointTimeoutError
    CALL_RUNTIME_ABSENT = 6,    // -> EndpointRuntimeUnavailableError (no Runtime attached)
    CALL_RUNTIME_DRAINING = 7,  // -> EndpointRuntimeUnavailableError (Runtime shutting down)
    CALL_OP_UNKNOWN = 8,        // -> EndpointOperationError (unknown op-code)
    CALL_OP_BADARGS = 9,        // -> EndpointValidationError (wrong arity / type)
    CALL_OP_FAILED = 10,        // -> EndpointOperationError (op body failed/cancelled)
    CALL_INTERNAL = 11          // -> EndpointOperationError (unexpected internal error)
};

// One fixed typed bridge argument as it travels on the wire / through the dispatch hook.
// tag: 0 = int64 (bits is the value), 1 = double (bits is the IEEE-754 bit pattern). v1 op
// codes accept int64 (tag 0) only; the double slot exists so the frame is value-model-shaped.
struct CallOpArg {
    int tag = 0;
    std::int64_t bits = 0;
};
// Result of a bridge dispatch: a status plus a typed (tag, bits) value on CALL_OK.
struct CallResult {
    int status = CALL_INTERNAL;
    int tag = 0;
    std::int64_t bits = 0;
};

// ===== Fixed-size wire codec (A1 transport) ================================
// Pure, HPX-free, socket-free byte (de)serialization for the A1 ping protocol. Big-
// endian integers, fixed sizes only -- NO variable-length payloads, blobs, or arbitrary
// data. Mirror of rayx/endpoint/_wire.py -- keep the layout and constants in sync. The
// actual local IPC (AF_UNIX) lives in endpoint_transport.hpp; this is just the bytes.
inline constexpr std::size_t WIRE_ID_LEN = 23;     // "rtb-ep-" + 16 hex, fixed
inline constexpr std::size_t WIRE_REQ_SIZE = 39;   // 4+2+1+1+23+8
inline constexpr std::size_t WIRE_RESP_SIZE = 16;  // 4+2+1+1+8
inline constexpr int WIRE_MSG_PING = 1;
// Server-side wire status (a subset reused by the Python ping status above).
inline constexpr int WIRE_OK = 0;
inline constexpr int WIRE_PEER_CLOSED = 1;
inline constexpr int WIRE_NOT_FOUND = 3;
inline constexpr int WIRE_PROTO_ERR = 4;

// ----- CALL_OP frames (bridge v1) -----------------------------------------
// A second fixed-size message type carried over the SAME AF_UNIX transport. The PING
// frames above are unchanged on the wire; serve_one reads a 7-byte prefix first
// (magic+proto+msg) and dispatches by msg, so PING bytes stay byte-for-byte compatible.
// Mirror of the CALL_OP codec in rayx/endpoint/_wire.py -- keep layouts in sync.
inline constexpr int WIRE_MSG_CALL_OP = 2;
inline constexpr std::size_t WIRE_PREFIX_SIZE = 7;       // magic(4)+proto(2)+msg(1)
inline constexpr std::size_t WIRE_MAX_CALL_ARGS = 2;     // fixed arg slots (square/add)
// request : 4+2+1+1 + 23(id) + 2(op_code) + 1(argc) + 1(rsv) + 2*(1 tag + 8 val) = 53
inline constexpr std::size_t WIRE_CALLOP_REQ_SIZE = 53;
// response: 4+2+1(status)+1(result_tag)+8(result_bits) = 16
inline constexpr std::size_t WIRE_CALLOP_RESP_SIZE = 16;

inline void wire_put_u16(unsigned char* p, std::uint16_t v) {
    p[0] = static_cast<unsigned char>((v >> 8) & 0xff);
    p[1] = static_cast<unsigned char>(v & 0xff);
}
inline std::uint16_t wire_get_u16(const unsigned char* p) {
    return static_cast<std::uint16_t>((static_cast<std::uint16_t>(p[0]) << 8) | p[1]);
}
inline void wire_put_i64(unsigned char* p, std::int64_t v) {
    std::uint64_t u = static_cast<std::uint64_t>(v);
    for (int i = 0; i < 8; ++i)
        p[i] = static_cast<unsigned char>((u >> (8 * (7 - i))) & 0xff);
}
inline std::int64_t wire_get_i64(const unsigned char* p) {
    std::uint64_t u = 0;
    for (int i = 0; i < 8; ++i) u = (u << 8) | p[i];
    return static_cast<std::int64_t>(u);
}

inline void wire_encode_request(int proto, const std::string& target_id,
                                std::int64_t nonce, unsigned char out[WIRE_REQ_SIZE]) {
    out[0] = 'R'; out[1] = 'A'; out[2] = 'Y'; out[3] = 'X';
    wire_put_u16(out + 4, static_cast<std::uint16_t>(proto));
    out[6] = static_cast<unsigned char>(WIRE_MSG_PING);
    out[7] = 0;
    std::memset(out + 8, 0, WIRE_ID_LEN);
    std::memcpy(out + 8, target_id.data(), std::min(target_id.size(), WIRE_ID_LEN));
    wire_put_i64(out + 8 + WIRE_ID_LEN, nonce);
}
// Returns false on a bad magic (caller closes the socket without replying).
inline bool wire_decode_request(const unsigned char in[WIRE_REQ_SIZE], int& proto,
                                int& msg, std::string& target_id, std::int64_t& nonce) {
    if (!(in[0] == 'R' && in[1] == 'A' && in[2] == 'Y' && in[3] == 'X')) return false;
    proto = wire_get_u16(in + 4);
    msg = in[6];
    target_id.assign(reinterpret_cast<const char*>(in + 8), WIRE_ID_LEN);
    nonce = wire_get_i64(in + 8 + WIRE_ID_LEN);
    return true;
}
inline void wire_encode_response(int proto, int status, std::int64_t value,
                                 unsigned char out[WIRE_RESP_SIZE]) {
    out[0] = 'R'; out[1] = 'A'; out[2] = 'Y'; out[3] = 'X';
    wire_put_u16(out + 4, static_cast<std::uint16_t>(proto));
    out[6] = static_cast<unsigned char>(status);
    out[7] = 0;
    wire_put_i64(out + 8, value);
}
inline bool wire_decode_response(const unsigned char in[WIRE_RESP_SIZE], int& proto,
                                 int& status, std::int64_t& value) {
    if (!(in[0] == 'R' && in[1] == 'A' && in[2] == 'Y' && in[3] == 'X')) return false;
    proto = wire_get_u16(in + 4);
    status = in[6];
    value = wire_get_i64(in + 8);
    return true;
}

// ----- CALL_OP codec (bridge v1) ------------------------------------------
// Decode just the fixed 7-byte prefix (magic+proto+msg). Returns false on bad magic; on
// success serve_one reads the rest of the frame for the indicated msg type (PING or
// CALL_OP), so the PING wire layout stays unchanged.
inline bool wire_decode_prefix(const unsigned char in[WIRE_PREFIX_SIZE], int& proto,
                               int& msg) {
    if (!(in[0] == 'R' && in[1] == 'A' && in[2] == 'Y' && in[3] == 'X')) return false;
    proto = wire_get_u16(in + 4);
    msg = in[6];
    return true;
}

inline void wire_encode_call_op(int proto, const std::string& target_id, int op_code,
                                const CallOpArg* args, int argc,
                                unsigned char out[WIRE_CALLOP_REQ_SIZE]) {
    std::memset(out, 0, WIRE_CALLOP_REQ_SIZE);
    out[0] = 'R'; out[1] = 'A'; out[2] = 'Y'; out[3] = 'X';
    wire_put_u16(out + 4, static_cast<std::uint16_t>(proto));
    out[6] = static_cast<unsigned char>(WIRE_MSG_CALL_OP);
    out[7] = 0;
    std::memcpy(out + 8, target_id.data(), std::min(target_id.size(), WIRE_ID_LEN));
    wire_put_u16(out + 31, static_cast<std::uint16_t>(op_code));
    if (argc < 0) argc = 0;
    if (static_cast<std::size_t>(argc) > WIRE_MAX_CALL_ARGS) argc = WIRE_MAX_CALL_ARGS;
    out[33] = static_cast<unsigned char>(argc);
    out[34] = 0;
    for (int i = 0; i < argc; ++i) {
        unsigned char* slot = out + 35 + i * 9;
        slot[0] = static_cast<unsigned char>(args[i].tag);
        wire_put_i64(slot + 1, args[i].bits);
    }
}
// Returns false on bad magic. `args` must hold WIRE_MAX_CALL_ARGS slots; argc says how
// many are meaningful (clamped to the max).
inline bool wire_decode_call_op(const unsigned char in[WIRE_CALLOP_REQ_SIZE], int& proto,
                                int& msg, std::string& target_id, int& op_code, int& argc,
                                CallOpArg* args) {
    if (!(in[0] == 'R' && in[1] == 'A' && in[2] == 'Y' && in[3] == 'X')) return false;
    proto = wire_get_u16(in + 4);
    msg = in[6];
    target_id.assign(reinterpret_cast<const char*>(in + 8), WIRE_ID_LEN);
    op_code = wire_get_u16(in + 31);
    argc = in[33];
    if (argc < 0) argc = 0;
    if (static_cast<std::size_t>(argc) > WIRE_MAX_CALL_ARGS) argc = WIRE_MAX_CALL_ARGS;
    for (std::size_t i = 0; i < WIRE_MAX_CALL_ARGS; ++i) {
        const unsigned char* slot = in + 35 + i * 9;
        args[i].tag = slot[0];
        args[i].bits = wire_get_i64(slot + 1);
    }
    return true;
}
inline void wire_encode_call_resp(int proto, int status, int result_tag,
                                  std::int64_t result_bits,
                                  unsigned char out[WIRE_CALLOP_RESP_SIZE]) {
    out[0] = 'R'; out[1] = 'A'; out[2] = 'Y'; out[3] = 'X';
    wire_put_u16(out + 4, static_cast<std::uint16_t>(proto));
    out[6] = static_cast<unsigned char>(status);
    out[7] = static_cast<unsigned char>(result_tag);
    wire_put_i64(out + 8, result_bits);
}
inline bool wire_decode_call_resp(const unsigned char in[WIRE_CALLOP_RESP_SIZE], int& proto,
                                  int& status, int& result_tag, std::int64_t& result_bits) {
    if (!(in[0] == 'R' && in[1] == 'A' && in[2] == 'Y' && in[3] == 'X')) return false;
    proto = wire_get_u16(in + 4);
    status = in[6];
    result_tag = in[7];
    result_bits = wire_get_i64(in + 8);
    return true;
}

// One endpoint's seam: identity + the fixed handshake handler. Held by shared_ptr in
// the registry; a Connection holds a weak_ptr so a closed/destroyed peer is observed
// as PEER_CLOSED rather than a dangling call.
struct EndpointSeam {
    std::string endpoint_id;
    long pid = 0;                 // owning process pid (cross-process discriminator)
    int proto_version = ENDPOINT_PROTO_VERSION;
    std::int64_t id_hash;         // FNV-1a of endpoint_id, cached at construction
    std::atomic<bool> closed{false};

    EndpointSeam(std::string id, long pid_)
        : endpoint_id(std::move(id)), pid(pid_),
          id_hash(endpoint_id_hash(endpoint_id)) {}

    // The fixed typed handshake. PEER-SPECIFIC: mixes THIS seam's id hash, so a
    // response from B's seam differs from C's for the same nonce (structural evidence
    // the call is parameterized by the resolved peer -- NOT cryptographic proof of
    // dispatch and NOT security). Pure and HPX-free; the binding (local registry path)
    // and the A1 transport listener both compute it INLINE -- no HPX call in the path.
    std::int64_t on_ping(std::int64_t nonce) const {
        return nonce ^ ENDPOINT_PING_XOR ^ id_hash;
    }
};

// Three-state result of a registry lookup. Lets the transport server distinguish a
// stale/never-registered id (ABSENT -> NotFound) from a previously valid endpoint that
// has since closed (CLOSED -> EndpointClosedError) -- even while the process listener is
// kept alive by ANOTHER transport endpoint in the same process.
enum LookupStatus { LOOKUP_ABSENT = 0, LOOKUP_LIVE = 1, LOOKUP_CLOSED = 2 };

// Process-local registry: endpoint_id -> seam. Plain mutex-guarded map (a lookup is
// NOT an HPX operation); single process only -- there is no cross-process visibility by
// construction. A `closed_` tombstone set records ids that were registered and then
// closed, so a closed peer reads as CLOSED rather than ABSENT while any endpoint lives
// (cleared by clear_tombstones() when the last endpoint closes / endpoint_shutdown()).
class EndpointRegistry {
public:
    static EndpointRegistry& instance() {
        static EndpointRegistry r;
        return r;
    }

    void register_seam(const std::shared_ptr<EndpointSeam>& s) {
        std::lock_guard<std::mutex> lk(mu_);
        map_[s->endpoint_id] = s;
        closed_.erase(s->endpoint_id);  // a fresh id is not a tombstone
    }

    void unregister(const std::string& id) {
        std::lock_guard<std::mutex> lk(mu_);
        map_.erase(id);
        closed_.insert(id);  // remember it as a closed peer (tombstone)
    }

    // Returns the live seam for `id`, or nullptr if not registered / already closed.
    // Used by the SAME-process connect path (the connection then observes a later close
    // via its weak_ptr); collapses CLOSED and ABSENT into nullptr by design.
    std::shared_ptr<EndpointSeam> resolve(const std::string& id) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = map_.find(id);
        if (it == map_.end()) return nullptr;
        if (it->second->closed.load(std::memory_order_relaxed)) return nullptr;
        return it->second;
    }

    // Three-state lookup for the cross-process transport server. On LOOKUP_LIVE, `out`
    // holds a shared_ptr keeping the seam alive for the (inline) compute.
    LookupStatus lookup(const std::string& id, std::shared_ptr<EndpointSeam>& out) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = map_.find(id);
        if (it != map_.end()) {
            if (it->second->closed.load(std::memory_order_relaxed))
                return LOOKUP_CLOSED;
            out = it->second;
            return LOOKUP_LIVE;
        }
        if (closed_.count(id)) return LOOKUP_CLOSED;
        return LOOKUP_ABSENT;
    }

    // Drop all closed-id tombstones. Called when the last endpoint closes (and by
    // endpoint_shutdown()), so tombstones never grow unbounded across endpoint cycles.
    void clear_tombstones() {
        std::lock_guard<std::mutex> lk(mu_);
        closed_.clear();
    }

private:
    EndpointRegistry() = default;
    std::mutex mu_;
    std::unordered_map<std::string, std::shared_ptr<EndpointSeam>> map_;
    std::unordered_set<std::string> closed_;  // tombstones: registered-then-closed ids
};

// Mint a fresh endpoint identity: rtb-ep- + exactly 16 lowercase hex (reuses the
// runtime's HPX-free id generator, only the prefix differs).
inline std::string make_endpoint_id() {
    return rayx_runtime::make_runtime_actor_id(ENDPOINT_ID_PREFIX);
}

}  // namespace rayx_endpoint

#endif  // RAYX_ENDPOINT_SEAM_HPP
