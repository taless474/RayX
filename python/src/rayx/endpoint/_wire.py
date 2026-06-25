"""rayx.endpoint A1 wire codec (import-light, stdlib-only).

Pure, socket-free (de)serialization of the fixed-size A1 ping frames. **Mirror of the
codec in ``python/src/rayx/endpoint_seam.hpp`` -- the layout and constants MUST stay in
sync.** Like ``_validate`` / ``_errors`` this module has no ``from .._rayx`` and no
relative imports (only ``struct``), so it loads by file path in lightweight unit tests.

The native transport (``endpoint_transport.hpp``) is the authority at runtime; this Python
codec exists for import-light testing and for cross-process test harnesses (e.g. a raw
AF_UNIX staller) that need to speak the same bytes without the extension.

Frames (big-endian, fixed size, NO variable-length payloads):

* request  (39 bytes): magic ``b"RAYX"`` | proto u16 | msg_type u8 | reserved u8 |
  target_endpoint_id 23 ASCII | nonce int64
* response (16 bytes): magic ``b"RAYX"`` | proto u16 | status u8 | reserved u8 |
  value int64

The bridge v1 CALL_OP frames (mirror endpoint_seam.hpp; a second message type carried over
the SAME transport -- PING bytes are unchanged):

* call request  (53 bytes): magic | proto u16 | msg_type u8 | reserved u8 |
  target_endpoint_id 23 ASCII | op_code u16 | argc u8 | reserved u8 | 2 x (tag u8 + bits q)
* call response (16 bytes): magic | proto u16 | status u8 | result_tag u8 | result_bits q
"""

import struct

__all__ = [
    "WIRE_MAGIC",
    "WIRE_PROTO_VERSION",
    "WIRE_MSG_PING",
    "WIRE_MSG_CALL_OP",
    "WIRE_ID_LEN",
    "WIRE_MAX_CALL_ARGS",
    "WIRE_REQ_FMT",
    "WIRE_RESP_FMT",
    "WIRE_REQ_SIZE",
    "WIRE_RESP_SIZE",
    "WIRE_CALLOP_REQ_FMT",
    "WIRE_CALLOP_RESP_FMT",
    "WIRE_CALLOP_REQ_SIZE",
    "WIRE_CALLOP_RESP_SIZE",
    "WIRE_OK",
    "WIRE_PEER_CLOSED",
    "WIRE_NOT_FOUND",
    "WIRE_PROTO_ERR",
    "encode_request",
    "decode_request",
    "encode_response",
    "decode_response",
    "encode_call_op",
    "decode_call_op",
    "encode_call_resp",
    "decode_call_resp",
]

WIRE_MAGIC = b"RAYX"
# Mirror ENDPOINT_PROTO_VERSION in endpoint_seam.hpp / _validate.py.
WIRE_PROTO_VERSION = 2
WIRE_MSG_PING = 1
WIRE_MSG_CALL_OP = 2
WIRE_ID_LEN = 23  # "rtb-ep-" + 16 hex, fixed
WIRE_MAX_CALL_ARGS = 2

# ">" = big-endian, no alignment padding. 4+2+1+1+23+8 = 39 ; 4+2+1+1+8 = 16.
WIRE_REQ_FMT = ">4sHBB23sq"
WIRE_RESP_FMT = ">4sHBBq"
WIRE_REQ_SIZE = struct.calcsize(WIRE_REQ_FMT)
WIRE_RESP_SIZE = struct.calcsize(WIRE_RESP_FMT)

# CALL_OP frames. request 4+2+1+1+23+2+1+1 + 2*(1+8) = 53 ; response 4+2+1+1+8 = 16.
WIRE_CALLOP_REQ_FMT = ">4sHBB23sHBB" + "Bq" * WIRE_MAX_CALL_ARGS
WIRE_CALLOP_RESP_FMT = ">4sHBBq"
WIRE_CALLOP_REQ_SIZE = struct.calcsize(WIRE_CALLOP_REQ_FMT)
WIRE_CALLOP_RESP_SIZE = struct.calcsize(WIRE_CALLOP_RESP_FMT)

# Server-side wire status codes (subset surfaced to Python ping status).
WIRE_OK = 0
WIRE_PEER_CLOSED = 1
WIRE_NOT_FOUND = 3
WIRE_PROTO_ERR = 4


def encode_request(proto, target_id, nonce):
    """Pack a ping request. ``target_id`` must be exactly 23 ASCII chars."""
    tid = target_id.encode("ascii")
    if len(tid) != WIRE_ID_LEN:
        raise ValueError(f"target id must be {WIRE_ID_LEN} bytes, got {len(tid)}")
    return struct.pack(WIRE_REQ_FMT, WIRE_MAGIC, proto, WIRE_MSG_PING, 0, tid, nonce)


def decode_request(buf):
    """Unpack a ping request -> (proto, msg_type, target_id, nonce). Raises ValueError on
    wrong size or bad magic."""
    if len(buf) != WIRE_REQ_SIZE:
        raise ValueError(f"request must be {WIRE_REQ_SIZE} bytes, got {len(buf)}")
    magic, proto, msg, _reserved, tid, nonce = struct.unpack(WIRE_REQ_FMT, buf)
    if magic != WIRE_MAGIC:
        raise ValueError("bad magic")
    return proto, msg, tid.decode("ascii"), nonce


def encode_response(proto, status, value):
    """Pack a ping response."""
    return struct.pack(WIRE_RESP_FMT, WIRE_MAGIC, proto, status, 0, value)


def decode_response(buf):
    """Unpack a ping response -> (proto, status, value). Raises ValueError on wrong size
    or bad magic."""
    if len(buf) != WIRE_RESP_SIZE:
        raise ValueError(f"response must be {WIRE_RESP_SIZE} bytes, got {len(buf)}")
    magic, proto, status, _reserved, value = struct.unpack(WIRE_RESP_FMT, buf)
    if magic != WIRE_MAGIC:
        raise ValueError("bad magic")
    return proto, status, value


def encode_call_op(proto, target_id, op_code, args):
    """Pack a bridge CALL_OP request. ``target_id`` must be exactly 23 ASCII chars;
    ``args`` is a list of (tag, bits) pairs (tag 0=int64, 1=double-bits), at most
    ``WIRE_MAX_CALL_ARGS``; unused slots are zero-filled."""
    tid = target_id.encode("ascii")
    if len(tid) != WIRE_ID_LEN:
        raise ValueError(f"target id must be {WIRE_ID_LEN} bytes, got {len(tid)}")
    if len(args) > WIRE_MAX_CALL_ARGS:
        raise ValueError(f"at most {WIRE_MAX_CALL_ARGS} args, got {len(args)}")
    argc = len(args)
    slots = list(args) + [(0, 0)] * (WIRE_MAX_CALL_ARGS - argc)
    flat = []
    for tag, bits in slots:
        flat.extend((tag, bits))
    return struct.pack(WIRE_CALLOP_REQ_FMT, WIRE_MAGIC, proto, WIRE_MSG_CALL_OP, 0,
                       tid, op_code, argc, 0, *flat)


def decode_call_op(buf):
    """Unpack a CALL_OP request -> (proto, msg_type, target_id, op_code, argc, args), where
    ``args`` is the list of ``argc`` (tag, bits) pairs. Raises ValueError on wrong size or
    bad magic."""
    if len(buf) != WIRE_CALLOP_REQ_SIZE:
        raise ValueError(
            f"call request must be {WIRE_CALLOP_REQ_SIZE} bytes, got {len(buf)}")
    fields = struct.unpack(WIRE_CALLOP_REQ_FMT, buf)
    magic, proto, msg, _rsv, tid, op_code, argc, _rsv2 = fields[:8]
    if magic != WIRE_MAGIC:
        raise ValueError("bad magic")
    rest = fields[8:]  # tag0, bits0, tag1, bits1, ...
    slots = [(rest[2 * i], rest[2 * i + 1]) for i in range(WIRE_MAX_CALL_ARGS)]
    argc = min(argc, WIRE_MAX_CALL_ARGS)
    return proto, msg, tid.decode("ascii"), op_code, argc, slots[:argc]


def encode_call_resp(proto, status, result_tag, result_bits):
    """Pack a CALL_OP response."""
    return struct.pack(WIRE_CALLOP_RESP_FMT, WIRE_MAGIC, proto, status,
                       result_tag, result_bits)


def decode_call_resp(buf):
    """Unpack a CALL_OP response -> (proto, status, result_tag, result_bits). Raises
    ValueError on wrong size or bad magic."""
    if len(buf) != WIRE_CALLOP_RESP_SIZE:
        raise ValueError(
            f"call response must be {WIRE_CALLOP_RESP_SIZE} bytes, got {len(buf)}")
    magic, proto, status, result_tag, result_bits = struct.unpack(
        WIRE_CALLOP_RESP_FMT, buf)
    if magic != WIRE_MAGIC:
        raise ValueError("bad magic")
    return proto, status, result_tag, result_bits
