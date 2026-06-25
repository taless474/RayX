"""Pure unit tests for rayx.endpoint validation + error hierarchy (experimental seam).

Import-light: the ``endpoint/_validate.py`` and ``endpoint/_errors.py`` modules are
loaded BY FILE PATH (see the fixtures below), so this suite runs WITHOUT the ``_rayx``
extension or HPX, in the lightweight repo-sanity CI job. The pure validators raise the
builtin ``ValueError`` / ``TypeError`` (the public ``rayx.endpoint`` layer re-raises
those as ``EndpointValidationError``); the typed-error mapping itself is covered by the
integration suite.
"""
import importlib.util
import pathlib

import pytest

_ENDPOINT_DIR = (pathlib.Path(__file__).resolve().parents[2]
                 / "python" / "src" / "rayx" / "endpoint")


def _load_by_path(name):
    path = _ENDPOINT_DIR / f"{name}.py"
    spec = importlib.util.spec_from_file_location(f"rayx_endpoint_unit_{name}", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def ev():
    """The import-light endpoint ``_validate`` module."""
    return _load_by_path("_validate")


@pytest.fixture(scope="module")
def ee():
    """The import-light endpoint ``_errors`` module."""
    return _load_by_path("_errors")


@pytest.fixture(scope="module")
def wire():
    """The import-light endpoint ``_wire`` codec module."""
    return _load_by_path("_wire")


def _good_meta(ev):
    return {"endpoint_id": "rtb-ep-0123456789abcdef", "pid": 4321,
            "host": "node-x", "proto_version": ev.ENDPOINT_PROTO_VERSION,
            "transport_kind": "none", "transport_addr": ""}


# --- endpoint id -------------------------------------------------------------


def test_valid_id_accepted(ev):
    assert ev.validate_endpoint_id("rtb-ep-0123456789abcdef") == \
        "rtb-ep-0123456789abcdef"


@pytest.mark.parametrize("bad", [
    "rtb-ep-0123456789ABCDEF",      # uppercase hex
    "rtb-ep-0123",                  # too short
    "rtb-ep-0123456789abcdef0",     # too long
    "rt-hpx-0123456789abcdef",      # wrong prefix
    "rtb-ep-0123456789abcdeg",      # non-hex
])
def test_bad_id_rejected(ev, bad):
    with pytest.raises(ValueError):
        ev.validate_endpoint_id(bad)


def test_non_str_id_rejected(ev):
    with pytest.raises(TypeError):
        ev.validate_endpoint_id(12345)


# --- metadata ----------------------------------------------------------------


def test_good_metadata_normalized(ev):
    out = ev.validate_metadata(_good_meta(ev))
    assert set(out) == set(ev.METADATA_KEYS)
    assert out["endpoint_id"] == "rtb-ep-0123456789abcdef"


def test_metadata_non_dict_rejected(ev):
    with pytest.raises(TypeError):
        ev.validate_metadata(["not", "a", "dict"])


def test_metadata_missing_key_rejected(ev):
    m = _good_meta(ev)
    del m["host"]
    with pytest.raises(ValueError):
        ev.validate_metadata(m)


def test_metadata_extra_key_rejected(ev):
    m = _good_meta(ev)
    m["port"] = 5000
    with pytest.raises(ValueError):
        ev.validate_metadata(m)


def test_metadata_bad_id_rejected(ev):
    m = _good_meta(ev)
    m["endpoint_id"] = "nope"
    with pytest.raises(ValueError):
        ev.validate_metadata(m)


def test_metadata_bool_pid_rejected(ev):
    m = _good_meta(ev)
    m["pid"] = True  # bool is an int subclass; must be rejected
    with pytest.raises(TypeError):
        ev.validate_metadata(m)


def test_metadata_empty_host_rejected(ev):
    m = _good_meta(ev)
    m["host"] = ""
    with pytest.raises(ValueError):
        ev.validate_metadata(m)


def test_metadata_proto_wrong_type_rejected(ev):
    m = _good_meta(ev)
    m["proto_version"] = "1"
    with pytest.raises(TypeError):
        ev.validate_metadata(m)


def test_proto_version_is_two(ev):
    assert ev.ENDPOINT_PROTO_VERSION == 2


# --- transport metadata (A1) -------------------------------------------------


def test_metadata_keys_include_transport(ev):
    assert set(ev.METADATA_KEYS) == {
        "endpoint_id", "pid", "host", "proto_version",
        "transport_kind", "transport_addr"}


def test_metadata_unix_transport_accepted(ev):
    m = _good_meta(ev)
    m["transport_kind"] = "unix"
    m["transport_addr"] = "/tmp/rayx-ep/ep-123.sock"
    out = ev.validate_metadata(m)
    assert out["transport_kind"] == "unix"
    assert out["transport_addr"] == "/tmp/rayx-ep/ep-123.sock"


def test_metadata_bad_transport_kind_rejected(ev):
    m = _good_meta(ev)
    m["transport_kind"] = "carrier-pigeon"
    with pytest.raises(ValueError):
        ev.validate_metadata(m)


def test_metadata_transport_kind_wrong_type_rejected(ev):
    m = _good_meta(ev)
    m["transport_kind"] = 1
    with pytest.raises(TypeError):
        ev.validate_metadata(m)


def test_metadata_transport_addr_wrong_type_rejected(ev):
    m = _good_meta(ev)
    m["transport_kind"] = "unix"
    m["transport_addr"] = 12345
    with pytest.raises(TypeError):
        ev.validate_metadata(m)


def test_metadata_none_kind_requires_empty_addr(ev):
    m = _good_meta(ev)
    m["transport_kind"] = "none"
    m["transport_addr"] = "/somewhere.sock"
    with pytest.raises(ValueError):
        ev.validate_metadata(m)


def test_metadata_unix_kind_requires_nonempty_addr(ev):
    m = _good_meta(ev)
    m["transport_kind"] = "unix"
    m["transport_addr"] = ""
    with pytest.raises(ValueError):
        ev.validate_metadata(m)


# --- nonce -------------------------------------------------------------------


def test_nonce_accepts_int64_range(ev):
    assert ev.validate_nonce(0) == 0
    assert ev.validate_nonce(ev.INT64_MAX) == ev.INT64_MAX
    assert ev.validate_nonce(ev.INT64_MIN) == ev.INT64_MIN


def test_nonce_bool_rejected(ev):
    with pytest.raises(TypeError):
        ev.validate_nonce(True)


def test_nonce_non_int_rejected(ev):
    with pytest.raises(TypeError):
        ev.validate_nonce(1.0)


def test_nonce_out_of_range_rejected(ev):
    with pytest.raises(ValueError):
        ev.validate_nonce(ev.INT64_MAX + 1)
    with pytest.raises(ValueError):
        ev.validate_nonce(ev.INT64_MIN - 1)


# --- endpoint_id_hash (peer-specific ping mixin) -----------------------------


def test_id_hash_deterministic(ev):
    assert ev.endpoint_id_hash("rtb-ep-0123456789abcdef") == \
        ev.endpoint_id_hash("rtb-ep-0123456789abcdef")


def test_id_hash_distinct_ids_distinct_hashes(ev):
    h1 = ev.endpoint_id_hash("rtb-ep-0123456789abcdef")
    h2 = ev.endpoint_id_hash("rtb-ep-fedcba9876543210")
    assert h1 != h2


def _ref_fnv1a_signed(s):
    """Independent FNV-1a 64-bit + signed-int64 reduction (the contract to match)."""
    h = 0xcbf29ce484222325
    for b in s.encode("ascii"):
        h = ((h ^ b) * 0x100000001b3) & ((1 << 64) - 1)
    return h - (1 << 64) if h >= (1 << 63) else h


def test_id_hash_matches_independent_fnv1a(ev):
    # Validates the algorithm AND the signed reinterpretation (covers both signs
    # whatever each input hashes to) against an independent implementation.
    for s in ("", "a", "rtb-ep-0123456789abcdef", "rtb-ep-fedcba9876543210",
              "rtb-ep-ffffffffffffffff"):
        v = ev.endpoint_id_hash(s)
        assert v == _ref_fnv1a_signed(s)
        assert ev.INT64_MIN <= v <= ev.INT64_MAX


def test_id_hash_empty_string_is_signed_basis_negative(ev):
    # FNV-1a of "" is the offset basis (high bit set) -> a NEGATIVE signed int64,
    # exercising the signed reduction explicitly.
    basis = 0xcbf29ce484222325
    expected = basis - (1 << 64)
    assert expected < 0
    assert ev.endpoint_id_hash("") == expected


# --- error hierarchy ---------------------------------------------------------


def test_error_hierarchy(ee):
    assert issubclass(ee.EndpointValidationError, ee.EndpointError)
    assert issubclass(ee.EndpointNotFoundError, ee.EndpointError)
    assert issubclass(ee.EndpointClosedError, ee.EndpointError)
    assert issubclass(ee.EndpointProtocolError, ee.EndpointError)
    assert issubclass(ee.EndpointUnreachableError, ee.EndpointError)
    assert issubclass(ee.EndpointTimeoutError, ee.EndpointError)
    # EndpointError is a RuntimeError; EndpointValidationError is also a ValueError.
    assert issubclass(ee.EndpointError, RuntimeError)
    assert issubclass(ee.EndpointValidationError, ValueError)
    # public-location identity for reprs/tracebacks/pickle
    assert ee.EndpointError.__module__ == "rayx.endpoint"
    assert ee.EndpointUnreachableError.__module__ == "rayx.endpoint"
    assert ee.EndpointTimeoutError.__module__ == "rayx.endpoint"


# --- wire codec (A1, pure import-light) --------------------------------------


def test_wire_frame_sizes(wire):
    assert wire.WIRE_REQ_SIZE == 39
    assert wire.WIRE_RESP_SIZE == 16
    assert wire.WIRE_PROTO_VERSION == 2
    assert wire.WIRE_MAGIC == b"RAYX"


@pytest.mark.parametrize("nonce", [0, 1, -7, 2 ** 40, -(2 ** 63), 2 ** 63 - 1])
def test_wire_request_round_trip(wire, nonce):
    tid = "rtb-ep-0123456789abcdef"
    buf = wire.encode_request(wire.WIRE_PROTO_VERSION, tid, nonce)
    assert len(buf) == wire.WIRE_REQ_SIZE
    proto, msg, got_id, got_nonce = wire.decode_request(buf)
    assert proto == wire.WIRE_PROTO_VERSION
    assert msg == wire.WIRE_MSG_PING
    assert got_id == tid
    assert got_nonce == nonce


@pytest.mark.parametrize("value", [0, 36, -1, 2 ** 50, -(2 ** 63), 2 ** 63 - 1])
def test_wire_response_round_trip(wire, value):
    buf = wire.encode_response(wire.WIRE_PROTO_VERSION, wire.WIRE_OK, value)
    assert len(buf) == wire.WIRE_RESP_SIZE
    proto, status, got = wire.decode_response(buf)
    assert proto == wire.WIRE_PROTO_VERSION
    assert status == wire.WIRE_OK
    assert got == value


def test_wire_request_bad_magic_rejected(wire):
    buf = wire.encode_request(wire.WIRE_PROTO_VERSION, "rtb-ep-0123456789abcdef", 1)
    bad = b"XXXX" + buf[4:]
    with pytest.raises(ValueError):
        wire.decode_request(bad)


def test_wire_request_short_rejected(wire):
    with pytest.raises(ValueError):
        wire.decode_request(b"RAYX")


def test_wire_response_short_rejected(wire):
    with pytest.raises(ValueError):
        wire.decode_response(b"RAYX\x00")


def test_wire_request_wrong_id_len_rejected(wire):
    with pytest.raises(ValueError):
        wire.encode_request(wire.WIRE_PROTO_VERSION, "too-short", 1)


# --- CALL_OP wire codec (bridge v1, pure import-light) ----------------------


def test_wire_callop_frame_sizes(wire):
    assert wire.WIRE_CALLOP_REQ_SIZE == 53
    assert wire.WIRE_CALLOP_RESP_SIZE == 16
    assert wire.WIRE_MSG_CALL_OP == 2
    assert wire.WIRE_MAX_CALL_ARGS == 2


@pytest.mark.parametrize("op_code,args", [
    (1, [(0, 7)]),                       # square(7)
    (2, [(0, 3), (0, 4)]),               # add(3, 4)
    (1, [(0, -(2 ** 63))]),              # int64 min edge
    (2, [(0, 2 ** 63 - 1), (0, -1)]),    # int64 max edge
])
def test_wire_callop_request_round_trip(wire, op_code, args):
    tid = "rtb-ep-0123456789abcdef"
    buf = wire.encode_call_op(wire.WIRE_PROTO_VERSION, tid, op_code, args)
    assert len(buf) == wire.WIRE_CALLOP_REQ_SIZE
    proto, msg, got_id, got_op, argc, got_args = wire.decode_call_op(buf)
    assert proto == wire.WIRE_PROTO_VERSION
    assert msg == wire.WIRE_MSG_CALL_OP
    assert got_id == tid
    assert got_op == op_code
    assert argc == len(args)
    assert got_args == args


@pytest.mark.parametrize("tag,bits", [(0, 49), (0, -25), (1, 0), (0, 2 ** 63 - 1)])
def test_wire_callop_response_round_trip(wire, tag, bits):
    buf = wire.encode_call_resp(wire.WIRE_PROTO_VERSION, wire.WIRE_OK, tag, bits)
    assert len(buf) == wire.WIRE_CALLOP_RESP_SIZE
    proto, status, got_tag, got_bits = wire.decode_call_resp(buf)
    assert proto == wire.WIRE_PROTO_VERSION
    assert status == wire.WIRE_OK
    assert got_tag == tag
    assert got_bits == bits


def test_wire_callop_too_many_args_rejected(wire):
    with pytest.raises(ValueError):
        wire.encode_call_op(wire.WIRE_PROTO_VERSION, "rtb-ep-0123456789abcdef", 1,
                            [(0, 1), (0, 2), (0, 3)])


def test_wire_callop_bad_magic_rejected(wire):
    buf = wire.encode_call_op(wire.WIRE_PROTO_VERSION, "rtb-ep-0123456789abcdef", 1, [(0, 1)])
    with pytest.raises(ValueError):
        wire.decode_call_op(b"XXXX" + buf[4:])
