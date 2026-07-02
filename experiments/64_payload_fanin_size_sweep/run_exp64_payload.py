#!/usr/bin/env python3
# exp64 -- payload-carrying fanin size sweep (EXPERIMENT-ONLY, Slice 0: pure layer).
#
# WHAT THIS IS
#   A synthetic, closed-oracle fanout/fanin where each remote leaf returns S opaque PAYLOAD BYTES
#   (plus its scalar closed-int64 value and locality witness) back across the Python caller boundary.
#   It extends the exp62 same-axis distributed direction from scalars to a payload-SIZE axis that
#   exp62 explicitly did not cover.
#
# WHAT THIS IS NOT
#   * NOT real inference. The payload is a deterministic synthetic byte pattern, not model output.
#   * NOT the shipped rayx.runtime API, NOT an object store, NOT arbitrary Python execution.
#   * NOT idiomatic/native HPX passive composition. exp64 rides the PROVEN poll-mode gather baseline
#     (root_flat_gather_poll). The native when_all/passive + collective/tree reduction direction stays
#     in the exp63 diagnostic arc and is NOT used here.
#   * NOT a same-axis Ray-vs-HPX *comparison* yet. Slice 0 is the pure oracle + design layer only;
#     no HPX build, no Ray runtime, no cluster. same_axis_comparison stays False.
#
# HPX FRAMING (durable)
#   The HPX path is the NAIVE ALL-TO-ROOT GATHER over a bounded is_ready poll -- the KNOWN-GOOD
#   reliable baseline, NOT "the HPX answer". Root receives O(N*S) bytes. The HPX-native target
#   (tree-of-partials / hpx::collectives reduce, folding O(S) at the root) is FUTURE work, as is
#   MPI/LCI/IB transport. Every exp64 HPX result must be labeled poll-mode gather baseline.
#
# TIMING BOUNDARY (durable)
#   To measure RESPONSE payload size at the Python caller boundary, the single timed blocking call
#   returns the payload BYTES (+ scalar values + witnesses) to Python. Python folds/checks the scalar
#   oracle and the payload digest AFTER timing, OUTSIDE the RTT window, identically for both arms. The
#   digest is NOT folded inside HPX/Ray, or the timed result would no longer carry response payload
#   size across the boundary.
#
# Slice 0 is Python-only and runs anywhere. The smoke / remote-smoke / size-sweep phases are skip
# stubs until the native (Slice 1+) and Ray/same-axis (Slice 3+) layers land.

import argparse
import json
import os
import sys
import time

# ---------------------------------------------------------------------------
# Closed-int64 scalar oracle -- identical base to exp62/exp63 shared_collective.hpp so a future exp64
# C++ leaf can reuse the same closed value model and only ADD the payload. int64 wrap is done via
# uint64 (mod 2^64) so the arithmetic is well-defined and portable to C++.
# ---------------------------------------------------------------------------

MASK64 = (1 << 64) - 1
LEAF_XOR = 0x52415958  # "RAYX"


def _to_int64(u):
    """Reinterpret a uint64 (mod 2^64) as signed int64."""
    u &= MASK64
    return u - (1 << 64) if u >= (1 << 63) else u


def leaf_value(x, i):
    """Per-leaf closed scalar: (x ^ "RAYX") + (i << 1), wrapped to int64. Matches exp63 leaf_value."""
    v = ((x & MASK64) ^ LEAF_XOR) + ((i & MASK64) << 1)
    return _to_int64(v)


def composite_oracle(x, n):
    """Order-independent int64 sum of leaf_value over i in [0, n), mod 2^64. Matches exp63."""
    acc = 0
    for i in range(n):
        acc = (acc + (((x & MASK64) ^ LEAF_XOR) + ((i & MASK64) << 1))) & MASK64
    return _to_int64(acc)


# ---------------------------------------------------------------------------
# Closed payload oracle. Each leaf produces S bytes; byte k is the low 8 bits of (uint64(leaf_value)+k),
# i.e. a per-leaf sawtooth of period 256. The payload DIGEST is the int64 (mod 2^64) sum of every byte
# over all leaves -- the closed value Python recomputes to verify the bytes that actually crossed the
# boundary. The payload stays a CLOSED value, not arbitrary bytes.
# ---------------------------------------------------------------------------

_FULL_PERIOD_BYTE_SUM = sum(range(256))  # 32640: sum of one full 0..255 period


def payload_byte(x, i, k):
    """The k-th payload byte of leaf i: low 8 bits of (uint64(leaf_value(x,i)) + k). In [0, 255]."""
    return ((leaf_value(x, i) & MASK64) + (k & MASK64)) & 0xFF


def _payload_digest_naive(x, n, s):
    """Reference digest: explicit byte-by-byte sum. O(n*S). Used to cross-check the fast path."""
    acc = 0
    for i in range(n):
        base = leaf_value(x, i) & MASK64
        for k in range(s):
            acc = (acc + ((base + k) & 0xFF)) & MASK64
    return _to_int64(acc)


def payload_digest(x, n, s):
    """Fast closed payload digest, equal to _payload_digest_naive but O(n * min(S, 256)).

    Per leaf the bytes are (b0+k) mod 256 for k in [0, S). Every full block of 256 consecutive k is a
    permutation of 0..255 (sum 32640) regardless of the b0 offset; only the trailing remainder depends
    on b0. So the per-leaf sum is full_blocks*32640 + sum of the < 256 remainder bytes.
    """
    if s < 0:
        raise ValueError("payload size S must be >= 0")
    acc = 0
    full, rem = divmod(s, 256)
    for i in range(n):
        b0 = leaf_value(x, i) & 0xFF
        leaf_sum = full * _FULL_PERIOD_BYTE_SUM
        for j in range(rem):
            leaf_sum += (b0 + j) & 0xFF
        acc = (acc + leaf_sum) & MASK64
    return _to_int64(acc)


def payload_bytes(x, i, s):
    """The full S-byte payload of leaf i as a bytes object (what a leaf returns across the boundary)."""
    base = leaf_value(x, i) & MASK64
    return bytes((base + k) & 0xFF for k in range(s))


# ---------------------------------------------------------------------------
# Corrected design record. Every field is a durable label enforcing the exp64 framing / fences. This
# is the provenance skeleton that native (Slice 1+) and same-axis (Slice 3+) runs will extend with
# measured transport/parcelport metadata -- never mutating the fence booleans below.
# ---------------------------------------------------------------------------

DEFAULT_SIZE_LADDER = [0, 64, 1024, 16384, 262144]

SIZE_LADDER_INTERPRETATION = {
    0: "poll+RTT+fixed-machinery floor, zero payload",
    64: "small payload",
    1024: "small payload",
    16384: "serialization/transport starts to matter",
    262144: "stress point; HPX/Ray transport regime change may appear",
}

EXP64_DESIGN = {
    "experiment": "exp64",
    "title": "payload-carrying fanin size sweep (poll-mode gather baseline)",
    "slice": 0,

    # payload identity
    "payload_mode": "response_fanin_python_boundary_payload",
    "payload_synthetic": True,
    "payload_is_model_output": False,
    "real_inference": False,

    # HPX composition framing (poll-mode gather baseline, NOT native/idiomatic, NOT collective/tree)
    "hpx_composition_mode": "root_flat_gather_poll",
    "hpx_composition_kind": "naive_all_to_root_gather_baseline",
    "poll_mode_baseline": True,
    "hpx_idiomatic_native_composition": False,
    "hpx_collective_or_tree_reduction": False,
    "hpx_gather_is_the_answer": False,  # it is the reliable baseline, not the answer
    "future_hpx_native_targets": ["tree_of_partials", "hpx_collectives_tree_reduce"],
    "future_transport_variants": ["mpi", "lci", "infiniband"],

    # payload C++ representation / transport metadata (design intent; measured values fill in Slice 1+)
    "payload_repr_cpp": "hpx::serialization::serialize_buffer<char>",
    "payload_repr_not": "naive std::vector<char> transport-facing",
    "serialize_buffer_mode": "record_at_slice1",
    "parcelport": "tcp",
    "transport": "tcp_eno16_10.42.5.x",
    "zero_copy_optimization": "record_at_runtime",
    "array_optimization": "record_at_runtime",
    "coalescing": "record_at_runtime",
    "size_thresholds": "record_if_known_or_observed",

    # timing boundary (payload bytes cross the Python boundary; fold happens AFTER timing)
    "timed_call_returns_payload_bytes_to_python": True,
    "digest_folded_inside_runtime": False,
    "digest_check_after_timing_outside_rtt": True,
    "fold_location_identical_across_arms": True,
    "post_timing_digest_check_cost_recorded_separately": True,

    # Ray transport caveat (do not read an HPX cause into a Ray-side regime change)
    "ray_transport_regime_may_change_with_size": True,
    "ray_object_store_plasma_possible_large_returns": True,
    "infer_hpx_cause_from_kink_requires_ray_transport_metadata": True,

    # size ladder
    "size_ladder_bytes": list(DEFAULT_SIZE_LADDER),
    "s0_is_floor": True,
    "s0_meaning": "poll+RTT+fixed machinery, zero payload",

    # fences (LOCKED False for the whole exp64 arc unless a future experiment explicitly permits)
    "speedup_computed": False,
    "ratio_reported": False,
    "arms_differenced": False,
    "placement_bands_differenced": False,
    "same_axis_comparison": False,

    # boundaries
    "is_public_rayx_runtime_api": False,
    "is_production": False,
}

# Fence booleans that MUST be False everywhere in the exp64 record.
FENCE_KEYS_FALSE = (
    "payload_is_model_output",
    "real_inference",
    "hpx_idiomatic_native_composition",
    "hpx_collective_or_tree_reduction",
    "hpx_gather_is_the_answer",
    "digest_folded_inside_runtime",
    "speedup_computed",
    "ratio_reported",
    "arms_differenced",
    "placement_bands_differenced",
    "same_axis_comparison",
    "is_public_rayx_runtime_api",
    "is_production",
)

# Substrings that must NOT appear in any record key -- forbidden speedup/superiority claims.
FORBIDDEN_KEY_SUBSTRINGS = (
    "speedup_value",
    "ratio_value",
    "hpx_beats",
    "ray_beats",
    "rayx_faster",
    "faster_than",
    "x_faster",
    "winner",
    "throughput_win",
    "latency_win",
)

# ---------------------------------------------------------------------------
# Timing-boundary + evidence-grade labels (durable). Both arms measure the SAME Python caller boundary
# with the SAME monotonic clock, and Slice 3 is an R=1 STRUCTURAL machinery pass -- NOT distributional
# evidence. same_axis_comparison is a structural-correlation flag the MANIFEST may set only if every
# gate passes; it is not a distributional/evidentiary claim and never licenses ratios/speedups/winners.
# ---------------------------------------------------------------------------

TIMING_BOUNDARY = "python_caller_monotonic_ns_around_blocking_call"
TIMING_CLOCK = "monotonic_ns"
EVIDENCE_GRADE_R1 = "structural_r1"

# Within-arm RTT summaries are OBSERVATIONS, never cross-arm arithmetic operands.
WITHIN_ARM_NOTE = "within-arm observation only; not distributional evidence; not a cross-arm operand"

# Honesty notes stamped into every manifest (recorded, not equalized, not differenced).
MANIFEST_HONESTY_NOTES = {
    "hpx_arm": "root_flat_gather_poll -- a polled payload gather baseline; "
               "NOT the exp63 native-validated composition",
    "ray_arm": "Ray coordinator + Ray object transport",
    "intentionally_different_runtime_paths": True,
    "transport_and_composition_recorded_not_equalized_not_differenced": True,
    "r1_is_structural_machinery_validation_only": True,
    "no_cross_arm_arithmetic": "manifest computes no ratios, no differences, no speedups, no winner",
}

# Manifest-level fences that MUST be False (same_axis_comparison is NOT here: the manifest may set it
# True as a structural-correlation flag when every gate passes).
MANIFEST_FENCE_KEYS_FALSE = (
    "arms_differenced",
    "ratio_reported",
    "speedup_computed",
    "placement_bands_differenced",
    "distributional_evidence",
    "percentiles_evidence_ready",
)

# ---------------------------------------------------------------------------
# Slice 4 band (R islands) labels. The band aggregate earns matched_band_r5 with WITHIN-ARM
# distributions only; it still computes NO cross-arm arithmetic. A stronger distributional_payload_ladder
# grade stays BLOCKED until the HPX serialization RUNTIME path is observed (config-level flags are
# observed; the per-call zero-copy path taken is not) -- so within-arm curves are honest but not yet a
# transport-attributed payload-scaling claim.
# ---------------------------------------------------------------------------

EVIDENCE_GRADE_BAND_R5 = "matched_band_r5"
DEFAULT_REQUIRED_ISLANDS = 5
DEFAULT_REQUIRED_MEASURED = 30

# Why a stronger distributional_payload_ladder grade is not earned here (recorded, not hidden).
DISTRIBUTIONAL_LADDER_BLOCKED_REASONS = (
    "hpx_serialization_runtime_path_not_observed",
    "hpx_poll_gather_baseline",
)

# HPX poll-gather provenance sourced from payload_ext.cpp (root_flat_gather_poll). The ext exposes the
# composition mode at runtime; the poll interval/yield is a documented SOURCE constant, not runtime
# introspection -- labeled as such so it never reads as an observed runtime measurement.
HPX_POLL_STRATEGY = "bounded_is_ready_poll_sleep_for"
HPX_POLL_INTERVAL_US = 50
HPX_POLL_PROVENANCE_SOURCE = ("payload_ext.cpp root_flat_gather_poll: bounded is_ready poll with a 50us "
                              "hpx::this_thread::sleep_for yield between checks; source constant, not "
                              "runtime-introspected")

# Coarse, DETERMINISTIC within-arm variability flags (labeled coarse; never a cross-arm comparison).
# high_variability: coefficient of variation (std/mean) over the measured RTTs exceeds the threshold.
# multimodal_suspected: the upper tail (p90-p50) dwarfs the lower spread (p50-min) -- a coarse skew/tail
# proxy, NOT a statistical modality test.
CV_HIGH_THRESHOLD = 0.5
MULTIMODAL_TAIL_RATIO = 3.0

# Band-level fences that MUST be False (same_axis_comparison may be True as a structural-correlation flag).
BAND_FENCE_KEYS_FALSE = (
    "arms_differenced",
    "ratio_reported",
    "speedup_computed",
    "placement_bands_differenced",
    "islands_cherry_picked",
)

BAND_HONESTY_NOTES = {
    "hpx_curve_context": "root_flat_gather_poll poll-gather baseline; the root deserializes/gathers "
                         "O(N*S) payload bytes on its pinned cores -- the large-S curve is root-gather "
                         "bound, NOT an HPX network-scaling claim",
    "ray_curve_context": "Ray coordinator + Ray object transport; object/plasma return path not_observed "
                         "unless detected",
    "across_island_spread_note": "across-island spread mixes physical placement (node/NIC/NUMA) and "
                                 "run-to-run jitter; it is not pure runtime jitter",
    "same_axis_meaning": "structural comparability across matched islands, NOT a timing verdict; no "
                         "ratio, speedup, difference, or winner is computed or implied",
    "distributional_scope": "within-arm distributions only; matched_band_r5 does not attribute payload "
                            "scaling to transport (serialization runtime path not_observed)",
}


def build_provenance(*, x, n, sizes, phase):
    """Assemble the exp64 provenance record for a (future) run. Slice 0 uses it for selftest gating."""
    rec = dict(EXP64_DESIGN)
    rec["x"] = int(x)
    rec["n"] = int(n)
    rec["size_ladder_bytes"] = [int(s) for s in sizes]
    rec["phase"] = phase
    rec["created_monotonic_ns"] = time.monotonic_ns()
    return rec


def validate_provenance(rec):
    """Fail-closed structural check of the exp64 fences/labels. Returns (ok, problems)."""
    problems = []
    for k in FENCE_KEYS_FALSE:
        if rec.get(k) is not False:
            problems.append(f"fence {k} must be False, got {rec.get(k)!r}")
    for k in rec:
        low = k.lower()
        for bad in FORBIDDEN_KEY_SUBSTRINGS:
            if bad in low:
                problems.append(f"forbidden claim key present: {k}")
    # required positive labels
    if rec.get("poll_mode_baseline") is not True:
        problems.append("poll_mode_baseline must be True")
    if rec.get("hpx_composition_kind") != "naive_all_to_root_gather_baseline":
        problems.append("hpx_composition_kind must be naive_all_to_root_gather_baseline")
    if rec.get("timed_call_returns_payload_bytes_to_python") is not True:
        problems.append("payload must cross the Python boundary")
    if rec.get("digest_check_after_timing_outside_rtt") is not True:
        problems.append("digest check must be after timing, outside RTT")
    if rec.get("payload_mode") != "response_fanin_python_boundary_payload":
        problems.append("payload_mode mislabeled")
    return (len(problems) == 0, problems)


# ---------------------------------------------------------------------------
# Payload digest fold + gates (PURE -- testable off-cluster)
# ---------------------------------------------------------------------------

def fold_payload_digest(leaves):
    """Python-side, POST-timing fold of the per-leaf response bytes into the closed int64 digest. This
    is the fold the runner does AFTER the timed call returns -- the runtime never folds the payload."""
    acc = 0
    for lf in leaves:
        acc = (acc + sum(lf["payload"])) & MASK64
    return _to_int64(acc)


def compute_payload_gates(*, x, n, payload_bytes, root_loc, remote_locs, result, folded_digest):
    """PURE per-call gate booleans for one payload gather result. root_loc is the embedded-root locality
    id; remote_locs are the resolved remote locality ids; result is the ext dict; folded_digest is the
    Python post-timing fold. All gates must be True for a call to pass."""
    leaves = result.get("leaves", [])
    localities = [int(lf["locality"]) for lf in leaves]
    leaves_local = sum(1 for l in localities if l == root_loc)
    leaves_remote = sum(1 for l in localities if l != root_loc)
    covered = set(localities)
    return {
        "n_leaves_dispatched": len(leaves) == n,
        "leaves_local_zero": leaves_local == 0,
        "leaves_remote_all": leaves_remote == n,
        "every_remote_locality_covered": bool(remote_locs) and all(rl in covered for rl in remote_locs),
        "witness_leaf_count_n": len(leaves) == n,
        "scalar_oracle_correct": int(result.get("composite")) == composite_oracle(x, n),
        "payload_byte_length_correct": all(int(lf["payload_len"]) == payload_bytes for lf in leaves)
        and all(len(lf["payload"]) == payload_bytes for lf in leaves),
        "payload_digest_correct": folded_digest == payload_digest(x, n, payload_bytes),
        "no_dispatch_timeout": int(result.get("timed_out_leaf_count", 1)) == 0,
    }


# ---------------------------------------------------------------------------
# HPX-only remote-smoke orchestration (Rostam-only; self-contained, no exp62/exp63 import)
# ---------------------------------------------------------------------------

EXP64_RUNS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_exp64_runs")
CONNECTOR_BIND_MODE = "none"


def _scontrol_hostnames(nodelist):
    import subprocess
    try:
        out = subprocess.run(["scontrol", "show", "hostnames", nodelist],
                             capture_output=True, text=True, timeout=15)
        if out.returncode == 0:
            return [h for h in out.stdout.split() if h]
    except Exception:  # noqa: BLE001
        pass
    return []


def _slurm_info(env=None):
    env = os.environ if env is None else env
    nodelist = env.get("SLURM_JOB_NODELIST")
    hostnames = _scontrol_hostnames(nodelist) if nodelist else []
    return {"slurm_job_id": env.get("SLURM_JOB_ID"), "nodelist": nodelist,
            "hostnames": hostnames, "n_nodes": len(hostnames)}


def _first_ip_for_subnet(prefer_subnet):
    if not prefer_subnet:
        return None
    import socket
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            ip = info[4][0]
            if ip.startswith(prefer_subnet):
                return ip
    except Exception:  # noqa: BLE001
        pass
    import subprocess
    try:
        out = subprocess.run(["hostname", "-I"], capture_output=True, text=True, timeout=10)
        if out.returncode == 0:
            for ip in out.stdout.split():
                if ip.startswith(prefer_subnet):
                    return ip
    except Exception:  # noqa: BLE001
        pass
    return None


def _short_host(h):
    return h.split(".")[0] if h else h


def _read_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:  # noqa: BLE001
        return {}


def _effective_cpuset():
    try:
        return sorted(os.sched_getaffinity(0))
    except Exception:  # noqa: BLE001
        return None


def _parse_cpulist(s):
    """Parse a Linux cpulist like '0-3,8,10-11' into a set of ints. Empty/garbage -> empty set."""
    out = set()
    for part in str(s or "").split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            a, _, b = part.partition("-")
            try:
                out.update(range(int(a), int(b) + 1))
            except ValueError:
                continue
        else:
            try:
                out.add(int(part))
            except ValueError:
                continue
    return out


def _iface_for_subnet(prefer_subnet):
    """Best-effort: the NIC whose IPv4 matches prefer_subnet, via Linux `ip -o -4 addr`. None on miss."""
    if not prefer_subnet:
        return None
    import subprocess
    try:
        out = subprocess.run(["ip", "-o", "-4", "addr", "show"], capture_output=True, text=True,
                             timeout=10)
        if out.returncode == 0:
            for line in out.stdout.splitlines():
                parts = line.split()
                if len(parts) >= 4 and parts[3].split("/")[0].startswith(prefer_subnet):
                    return parts[1]
    except Exception:  # noqa: BLE001
        pass
    return None


def _numa_nic_provenance(prefer_subnet, root_cpuset):
    """Best-effort NUMA/NIC provenance (Linux `/sys`, `ip`). PROVENANCE ONLY -- every field is
    'not_observed' off-Linux or when a source is unavailable; this never raises and never gates."""
    prov = {"selected_iface": "not_observed", "nic_numa_node": "not_observed",
            "root_core_numa_nodes": "not_observed", "numa_nic_colocated": "not_observed"}
    iface = _iface_for_subnet(prefer_subnet)
    if iface:
        prov["selected_iface"] = iface
        try:
            with open(f"/sys/class/net/{iface}/device/numa_node") as f:
                prov["nic_numa_node"] = int(f.read().strip())
        except Exception:  # noqa: BLE001
            pass
    try:
        if root_cpuset:
            import glob as _glob
            import re as _re
            core_node = {}
            for nd in _glob.glob("/sys/devices/system/node/node[0-9]*"):
                m = _re.search(r"node(\d+)$", nd)
                if not m:
                    continue
                try:
                    with open(os.path.join(nd, "cpulist")) as f:
                        cpus = _parse_cpulist(f.read())
                except Exception:  # noqa: BLE001
                    continue
                for cpu in cpus:
                    core_node[cpu] = int(m.group(1))
            nodes = sorted({core_node[c] for c in root_cpuset if c in core_node})
            if nodes:
                prov["root_core_numa_nodes"] = nodes
                if isinstance(prov["nic_numa_node"], int):
                    prov["numa_nic_colocated"] = prov["nic_numa_node"] in nodes
    except Exception:  # noqa: BLE001
        pass
    return prov


def build_root_hpx_args(*, root_ip, root_port):
    """Embedded-root HPX args: bind the root parcelport/AGAS to the selected-subnet IP:port and expect
    connecting localities. Mirrors the proven exp63 root join flags (balanced bind)."""
    return ["--hpx:ignore-batch-env",
            f"--hpx:hpx={root_ip}:{root_port}",
            f"--hpx:agas={root_ip}:{root_port}",
            "--hpx:expect-connecting-localities",
            "--hpx:bind=balanced"]


def build_connector_srun_cmd(rhost, bootstrap_dir, *, connector_bin, connector_threads, serve_timeout,
                             prefer_subnet, root_ip, root_port):
    """The srun command that launches one payload_connector on its own node. --overlap + --cpu-bind=none
    so the connector step binds cleanly WITHIN its node allocation regardless of the root step's
    --cpus-per-task (the proven exp63 connector-launch shape)."""
    return ["srun", "--overlap", "--cpu-bind=none", "--nodes=1", "--ntasks=1",
            f"--nodelist={rhost}", f"--cpus-per-task={connector_threads}",
            connector_bin, "--role=connect", f"--bootstrap={bootstrap_dir}",
            f"--serve-timeout={serve_timeout}", f"--prefer-subnet={prefer_subnet}",
            f"--agas-preprobe-host={root_ip}", f"--agas-preprobe-port={root_port}",
            f"--hpx:threads={connector_threads}", "--hpx:bind=none",
            "--hpx:ignore-batch-env", f"--hpx:agas={root_ip}:{root_port}"]


def _default_payload_import():
    here = os.path.dirname(os.path.abspath(__file__))
    for d in (os.path.join(here, "build"), os.path.join(here, "build", "Release")):
        if os.path.isdir(d) and d not in sys.path:
            sys.path.insert(0, d)
    import payload_ext
    return payload_ext


def _find_connector_bin(here=None):
    here = os.path.dirname(os.path.abspath(__file__)) if here is None else here
    for d in (os.path.join(here, "build"), os.path.join(here, "build", "Release")):
        cand = os.path.join(d, "payload_connector")
        if os.path.isfile(cand):
            return cand
    return None


def _payload_preconditions(env, import_fn=None):
    """Resolve (payload_ext, slurm, connector_bin) or a SKIP reason. The 3-node all-remote 4/4 payload
    shape needs a >=3-node Slurm allocation (root + 2 connectors); off-cluster and unbuilt both skip."""
    slurm = _slurm_info(env)
    if len(slurm["hostnames"]) < 3:
        return None, (f"SKIP -- need a >=3-node Slurm allocation (root + 2 connectors) for the 3-node "
                      f"all-remote payload shape (hostnames={slurm['hostnames']}).")
    connector_bin = _find_connector_bin()
    if connector_bin is None:
        return None, "SKIP -- payload_connector not built (build the exp64 CMake target first)."
    try:
        ext = (import_fn or _default_payload_import)()
    except ImportError as exc:
        return None, f"SKIP -- payload_ext not built ({exc})."
    return (ext, slurm, connector_bin), None


def _size_calls(ext, *, x, n, payload_bytes, dispatch_timeout_s, root_loc, remote_locs, prewarm,
                measured):
    """Run prewarm + `measured` TIMED payload gathers for ONE size against an ALREADY-STARTED root
    with connectors joined. Times each call, folds+checks the digest AFTER timing. Returns the calls."""
    for _ in range(prewarm):
        ext.fanout_fanin_payload_remote(x, n, payload_bytes, dispatch_timeout_s)
    calls = []
    for c in range(measured):
        t0 = time.monotonic_ns()
        result = dict(ext.fanout_fanin_payload_remote(x, n, payload_bytes, dispatch_timeout_s))
        t1 = time.monotonic_ns()  # RTT boundary ends here: payload bytes are back in Python
        # Fold + check the digest AFTER timing, OUTSIDE the RTT window.
        d0 = time.monotonic_ns()
        folded = fold_payload_digest(result["leaves"])
        d1 = time.monotonic_ns()
        gates = compute_payload_gates(x=x, n=n, payload_bytes=payload_bytes, root_loc=root_loc,
                                      remote_locs=remote_locs, result=result, folded_digest=folded)
        calls.append({
            "call_index": c,
            "rtt_ns": t1 - t0, "rtt_ms": (t1 - t0) / 1e6,
            "digest_check_ns": d1 - d0, "digest_check_ms": (d1 - d0) / 1e6,
            "folded_digest": folded, "expected_digest": payload_digest(x, n, payload_bytes),
            "composite": int(result["composite"]),
            "timed_out_leaf_count": int(result.get("timed_out_leaf_count", 0)),
            "n_localities": int(result.get("n_localities", 0)),
            "leaf_localities": [int(lf["locality"]) for lf in result.get("leaves", [])],
            "gates": gates, "gates_pass": all(gates.values()),
        })
    return calls


def _drive_payload_remote_smoke(ext, slurm, connector_bin, *, x, n, sizes, dispatch_timeout_s,
                                prefer_subnet, root_port, await_timeout, serve_timeout, n_remote,
                                prewarm, measured, env=None):
    """Bring up the embedded HPX root + 2 connectors ONCE (HPX does not support restart in-process),
    serve ALL sizes in that single connector window (S=0 first; stop before dispatching a larger S if
    an earlier one fails), then disconnect. Returns a LIST of per-size artifacts sharing the same
    connector provenance. Rostam-only."""
    import subprocess
    import tempfile

    env = os.environ if env is None else env
    connector_threads = 8
    root_hpx_threads = 4
    hostnames = slurm["hostnames"]
    root_host = hostnames[0]
    remote_hosts = [h for h in hostnames if h != root_host][:n_remote]
    root_ip = _first_ip_for_subnet(prefer_subnet)
    os.makedirs(EXP64_RUNS, exist_ok=True)

    procs, bootdirs = [], []
    started = False
    error = None
    root_loc = None
    root_cpuset = None
    hpx_config = None
    remote_locs = []
    per_size = []          # list of (S, calls) in run order
    failed_before = None   # size whose gates failed -> stop dispatching larger sizes
    try:
        if len(remote_hosts) < 2:
            raise RuntimeError(f"need >=2 remote hosts distinct from root; got {remote_hosts}")
        if not root_ip:
            raise RuntimeError(f"no root IP for --prefer-subnet {prefer_subnet}")
        ext.start(root_hpx_threads, build_root_hpx_args(root_ip=root_ip, root_port=root_port))
        started = True
        root_loc = int(ext.local_locality_id())
        root_cpuset = _effective_cpuset()
        try:
            hpx_config = dict(ext.hpx_config_provenance())
        except Exception:  # noqa: BLE001
            hpx_config = None
        for idx, rhost in enumerate(remote_hosts):
            bd = tempfile.mkdtemp(
                prefix=f"payload_{slurm['slurm_job_id']}_c{idx + 1}_", dir=EXP64_RUNS)
            bootdirs.append(bd)
            cmd = build_connector_srun_cmd(
                rhost, bd, connector_bin=connector_bin, connector_threads=connector_threads,
                serve_timeout=serve_timeout, prefer_subnet=prefer_subnet, root_ip=root_ip,
                root_port=root_port)
            procs.append(subprocess.Popen(cmd))
        joined = int(ext.await_remotes(len(remote_hosts), await_timeout))
        if joined < len(remote_hosts):
            raise RuntimeError(f"only {joined} of {len(remote_hosts)} remote localities joined")
        remote_locs = [int(v) for v in ext.remote_locality_ids()]
        for s in sizes:
            calls = _size_calls(ext, x=x, n=n, payload_bytes=int(s),
                                dispatch_timeout_s=dispatch_timeout_s, root_loc=root_loc,
                                remote_locs=remote_locs, prewarm=prewarm, measured=measured)
            per_size.append((int(s), calls))
            if not (calls and all(cc["gates_pass"] for cc in calls)):
                failed_before = int(s)  # do not dispatch any larger S in this window
                break
    except Exception as exc:  # noqa: BLE001
        error = f"{type(exc).__name__}: {exc}"
    finally:
        for bd in bootdirs:
            try:
                with open(os.path.join(bd, "served1.ok"), "w") as fh:
                    fh.write("served\n")
            except OSError:
                pass
        for p in procs:
            try:
                p.wait(timeout=max(10, serve_timeout))
            except Exception:  # noqa: BLE001
                p.kill()
        if started:
            try:
                ext.shutdown()
            except Exception as exc:  # noqa: BLE001
                error = error or f"shutdown: {type(exc).__name__}: {exc}"

    connectors = []
    for idx, bd in enumerate(bootdirs):
        att = _read_json(os.path.join(bd, "attest_connect.json"))
        disc = _read_json(os.path.join(bd, "connect.disconnected1"))
        cpuset = att.get("connector_cpuset_effective")
        connectors.append({
            "bootstrap_dir": os.path.basename(bd),
            "hostname": _short_host(att.get("hostname")
                                    or (remote_hosts[idx] if idx < len(remote_hosts) else None)),
            "locality_id": att.get("locality_id"),
            "connector_cpuset_effective": cpuset,
            "cpuset_not_collapsed": bool(cpuset) and len(cpuset) > 1,
            "tcp_nodelay_verified": bool(att.get("tcp_nodelay_attested") and att.get("tcp_nodelay")),
            "joined": os.path.isfile(os.path.join(bd, "connect.joined1")),
            "served": os.path.isfile(os.path.join(bd, "served1.ok")),
            "graceful_disconnect": os.path.isfile(os.path.join(bd, "connect.disconnected1"))
            and bool(disc.get("served")),
        })

    connector_lifecycle_ok = bool(connectors) and all(
        c["joined"] and c["served"] and c["graceful_disconnect"] for c in connectors)
    cpuset_not_collapsed = bool(connectors) and all(c["cpuset_not_collapsed"] for c in connectors)
    nodelay_verified = bool(connectors) and all(c["tcp_nodelay_verified"] for c in connectors)
    fences_ok, _ = validate_provenance(EXP64_DESIGN)

    hpx_teardown_clean = connector_lifecycle_ok and error is None  # residue-clear evidence for the Ray phase

    # ---- island-level provenance folded from the HPX/runtime review (same for every size here) ----
    served_all = bool(connectors) and all(c["served"] for c in connectors)
    connector_anomaly_witness = {
        "connector_lifecycle_ok": connector_lifecycle_ok,
        "connector_shutdown_reason": "served_signal" if served_all
        else ("serve_timeout_or_error" if connectors else "not_observed"),
        "serve_timeout_expired_any": (any(not c["served"] for c in connectors)
                                      if connectors else "not_observed"),
        "connector_stayed_alive_until_root_done": served_all,
        "late_parcel_after_shutdown_detected": "not_observed",  # exp64 connector has no such witness
        "heartbeat_anomaly_detected": "not_observed",           # exp64 connector has no heartbeat field
    }
    cfg = hpx_config or {}
    hpx_serialization = {
        "payload_representation": EXP64_DESIGN["payload_repr_cpp"],
        "serialize_buffer_construction_mode": "not_observed",
        "zero_copy_optimization_config": cfg.get("hpx.parcel.tcp.zero_copy_optimization", "not_observed"),
        "zero_copy_receive_optimization_config":
            cfg.get("hpx.parcel.tcp.zero_copy_receive_optimization", "not_observed"),
        "array_optimization_config": cfg.get("hpx.parcel.tcp.array_optimization", "not_observed"),
        "coalescing_message_handlers_config": cfg.get("hpx.parcel.message_handlers", "not_observed"),
        "max_message_size_config": cfg.get("hpx.parcel.max_message_size", "not_observed"),
        "max_outbound_message_size_config":
            cfg.get("hpx.parcel.max_outbound_message_size", "not_observed"),
        "parcel_pool_size_config": cfg.get("hpx.parcel.tcp.parcel_pool_size", "not_observed"),
        # config-level flags are OBSERVED; the per-call zero-copy path TAKEN is NOT -> blocks a stronger grade
        "zero_copy_runtime_path_taken": "not_observed",
    }
    hpx_poll = {
        "hpx_composition": "root_flat_gather_poll",
        "hpx_composition_context": "poll_gather_payload_baseline",
        "hpx_not_exp63_native_composition": True,
        "hpx_poll_strategy": HPX_POLL_STRATEGY,
        "hpx_poll_interval_us": HPX_POLL_INTERVAL_US,
        "hpx_poll_yield_mechanism": "hpx_this_thread_sleep_for",
        "hpx_poll_provenance_source": HPX_POLL_PROVENANCE_SOURCE,
    }
    hpx_runtime = {
        "hpx_threads": cfg.get("hpx.threads", str(root_hpx_threads)),
        "hpx_root_threads": root_hpx_threads,
        "hpx_connector_threads": connector_threads,
        "hpx_bind": "balanced",          # root started with --hpx:bind=balanced
        "hpx_connector_bind": "none",    # connectors launched with --hpx:bind=none
        "hpx_root_effective_cpuset": root_cpuset,
        "hpx_connector_effective_cpusets": [c["connector_cpuset_effective"] for c in connectors],
        "hpx_parcel_pool_size": cfg.get("hpx.parcel.tcp.parcel_pool_size", "not_observed"),
        "hpx_message_handlers": cfg.get("hpx.parcel.message_handlers", "not_observed"),
        "hpx_max_background_threads": cfg.get("hpx.max_background_threads", "not_observed"),
    }
    numa_nic = _numa_nic_provenance(prefer_subnet, root_cpuset)

    r_remote = len(remote_locs)
    expected_each = sorted(n // r_remote + (1 if j < n % r_remote else 0) for j in range(r_remote)) \
        if r_remote else []

    def _hpx_all_remote(cc):
        g = cc.get("gates", {})
        return bool(g.get("leaves_local_zero")) and bool(g.get("leaves_remote_all"))

    def _hpx_balanced(cc):
        if not remote_locs:
            return False
        counts = {}
        for loc in cc.get("leaf_localities", []):
            counts[loc] = counts.get(loc, 0) + 1
        got = sorted(counts.get(rl, 0) for rl in remote_locs)
        return got == expected_each and sum(counts.get(rl, 0) for rl in remote_locs) == n

    artifacts = []
    for payload_bytes, calls in per_size:
        all_calls_pass = bool(calls) and all(c["gates_pass"] for c in calls)
        all_remote_all_calls = bool(calls) and all(_hpx_all_remote(c) for c in calls)
        distribution_balanced_all_calls = bool(calls) and all(_hpx_balanced(c) for c in calls)
        dispatch_no_timeout_all_calls = bool(calls) and all(
            c["gates"].get("no_dispatch_timeout") is True for c in calls)
        structural_gates = {
            "all_calls_gates_pass": all_calls_pass,
            "connector_joined_served_graceful": connector_lifecycle_ok,
            "cpuset_not_collapsed": cpuset_not_collapsed,
            "tcp_nodelay_verified": nodelay_verified,
            "timed_call_returns_payload_bytes_to_python": True,
            "digest_folded_inside_runtime": False,
            "digest_check_after_timing_outside_rtt": True,
            "fences_locked_false": fences_ok,
            "same_axis_comparison_false": EXP64_DESIGN["same_axis_comparison"] is False,
        }
        overall_pass = (error is None and all_calls_pass and connector_lifecycle_ok
                        and cpuset_not_collapsed and nodelay_verified)
        rec = build_provenance(x=x, n=n, sizes=[payload_bytes], phase="hpx-payload-remote-smoke")
        rec.update({
            "kind": "hpx_payload_remote_smoke",
            "arm": "hpx",
            "slice": 1,
            "payload_bytes": payload_bytes,
            "payload_representation": EXP64_DESIGN["payload_repr_cpp"],
            "hpx_native_collective": False,
            "payload_is_synthetic": True,
            "payload_not_model_output": True,
            "no_inference": True,
            "future_targets": list(EXP64_DESIGN["future_hpx_native_targets"])
            + list(EXP64_DESIGN["future_transport_variants"]),
            "n_remote": n_remote,
            "dispatch_timeout_s": dispatch_timeout_s,
            "prewarm": prewarm, "measured": measured,
            "root_port": root_port, "serve_timeout_s": serve_timeout, "await_timeout_s": await_timeout,
            "prefer_subnet": prefer_subnet,
            "selected_subnet": prefer_subnet,
            "root_ip": root_ip,
            "root_locality": root_loc,
            "remote_locality_ids": remote_locs,
            "root_effective_cpuset": root_cpuset,
            "node_set": [_short_host(root_host)] + [c["hostname"] for c in connectors],
            "connectors": connectors,
            "hpx_config": hpx_config,
            "expected_digest": payload_digest(x, n, payload_bytes),
            "calls": calls,
            "structural_gates": structural_gates,
            "overall_pass": overall_pass,
            "error": error,
            "slurm_job_id": slurm["slurm_job_id"],

            # timing boundary + evidence grade (folded from HPX/runtime review)
            "boundary": TIMING_BOUNDARY,
            "clock": TIMING_CLOCK,
            "evidence_grade": EVIDENCE_GRADE_R1,
            "distributional_evidence": False,
            "percentiles_evidence_ready": False,

            # HPX composition/transport OBSERVATIONS (recorded provenance, not tuned knobs)
            "hpx_composition": "root_flat_gather_poll",
            "hpx_composition_note": "polled gather baseline; not exp63 native-validated composition",
            "hpx_parcelport": "tcp",
            "transport_family": "tcp",
            "tcp_nodelay": nodelay_verified,
            "serialize_buffer_mode": "serialize_buffer<char>",
            "zero_copy_optimization": "not_observed",
            "array_optimization": "not_observed",
            "coalescing": "not_observed",
            "size_thresholds": "not_observed",
            "connector_lifecycle_ok": connector_lifecycle_ok,

            # cross-phase manifest evidence
            "effective_cpu_binding": root_cpuset,
            "phase_affinity_recorded": root_cpuset is not None,
            "hpx_teardown_clean": hpx_teardown_clean,
            "prewarm_excluded_from_timed": True,
            "all_remote_all_calls": all_remote_all_calls,
            "distribution_balanced_all_calls": distribution_balanced_all_calls,
            "dispatch_no_timeout_all_calls": dispatch_no_timeout_all_calls,

            # Slice 4 review-folded provenance (island-level; recorded, not tuned/equalized/differenced)
            "hpx_poll": hpx_poll,
            "hpx_runtime": hpx_runtime,
            "hpx_serialization": hpx_serialization,
            "numa_nic": numa_nic,
            "connector_anomaly_witness": connector_anomaly_witness,
        })
        artifacts.append(rec)
    return artifacts, failed_before


def _mean_rtt_ms(art):
    calls = art.get("calls", [])
    return (sum(c["rtt_ms"] for c in calls) / len(calls)) if calls else float("nan")


def run_payload_remote_smoke(*, x=7, n=8, sizes=(0, 262144), env=None, import_fn=None, write=True,
                             dispatch_timeout_s=8.0, prefer_subnet="10.42.5.", root_port=7950,
                             await_timeout=60, serve_timeout=600, n_remote=2, prewarm=3, measured=5):
    """Run the HPX-only payload remote-smoke over `sizes` in ONE HPX/connector lifecycle, S=0 first.
    Larger sizes are not dispatched once an earlier size fails. Skips cleanly off-cluster/unbuilt."""
    pre, skip = _payload_preconditions(env or os.environ, import_fn)
    if skip:
        print("exp64 hpx-payload-remote-smoke:", skip)
        return 0
    ext, slurm, connector_bin = pre
    os.makedirs(EXP64_RUNS, exist_ok=True)
    artifacts, failed_before = _drive_payload_remote_smoke(
        ext, slurm, connector_bin, x=x, n=n, sizes=[int(s) for s in sizes],
        dispatch_timeout_s=dispatch_timeout_s, prefer_subnet=prefer_subnet, root_port=root_port,
        await_timeout=await_timeout, serve_timeout=serve_timeout, n_remote=n_remote,
        prewarm=prewarm, measured=measured, env=env)
    rc = 0
    for art in artifacts:
        s = art["payload_bytes"]
        if write:
            path = os.path.join(EXP64_RUNS,
                                f"exp64_payload_smoke_{slurm['slurm_job_id']}_S{s}_hpx.json")
            with open(path, "w") as f:
                json.dump(art, f, indent=2)
        ok = bool(art.get("overall_pass"))
        print(f"exp64 payload-smoke S={s}: overall_pass={ok} error={art.get('error')} "
              f"calls={len(art.get('calls', []))} mean_rtt_ms={_mean_rtt_ms(art):.3f} "
              f"remote_locs={art.get('remote_locality_ids')}")
        if write:
            print(f"  wrote exp64_payload_smoke_{slurm['slurm_job_id']}_S{s}_hpx.json (gitignored)")
        if not ok:
            rc = 1
    if failed_before is not None:
        print(f"exp64 payload-smoke: S={failed_before} did NOT pass; larger sizes not dispatched.")
    return rc


# ---------------------------------------------------------------------------
# Ray matched smoke arm (Slice 2). A Ray actor-coordinator payload fanin measured at the SAME Python
# caller boundary as the HPX arm. Ray-PATTERN mapping, NOT HPX best practice. Topology mirrors exp62
# Slice 4b: head/coordinator on node A (num_cpus=0, runs ZERO leaves) + two remote worker nodes; N
# leaves hard-pinned ROUND-ROBIN across the two remote Ray worker node ids (4/4), all leaves remote.
#
# ONE blocking call per timed iteration: ray.get(coordinator.remote(x, n, payload_bytes)). The
# coordinator returns the RAW payload BYTES (+ scalar values + node witnesses) to Python; Python stops
# the RTT clock when the bytes return and folds/checks the scalar oracle + payload digest AFTER timing,
# OUTSIDE the RTT window -- identical fold location to the HPX arm. The coordinator NEVER folds the
# digest for timing. Mechanism smoke only: no ratios, no speedups, no winner; same_axis_comparison stays
# False (a single matched smoke does not structurally earn a same-axis comparison).
# ---------------------------------------------------------------------------

def _default_ray_import():
    import ray
    return ray


def _sh(cmd, timeout=120, env=None):
    import subprocess
    try:
        p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout,
                           env=env, text=True)
        return p.returncode, p.stdout, p.stderr
    except Exception as exc:  # noqa: BLE001
        return 1, "", f"[exec error] {exc}"


def _node_ip_on_subnet(node, prefer_subnet, env=None):
    """First IPv4 of `node` matching prefer_subnet, via `srun --overlap hostname -I`. None on miss."""
    rc, out, _ = _sh(["srun", "-N1", "-n1", "--overlap", "--nodelist", node, "hostname", "-I"],
                     timeout=60, env=env)
    if rc != 0:
        return None
    for tok in out.split():
        if tok.count(".") == 3 and tok.startswith(prefer_subnet):
            return tok
    return None


def _ray_popen_block(cmd, env, log_path):
    """Launch a PERSISTENT `ray start --block` srun step via Popen so Slurm does not reap the Ray
    daemons (exp59/exp62 launcher-lifetime fix)."""
    import subprocess
    lf = open(log_path, "ab", buffering=0)
    p = subprocess.Popen(cmd, stdout=lf, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                         env=env, start_new_session=True)
    p._exp64_logfile = lf
    return p


def _ray_stop_node(node, env):
    return _sh(["srun", "-N1", "-n1", "--overlap", "--nodelist", node, "ray", "stop", "--force"],
               timeout=90, env=env)


def _orphan_check_node(node, env, patterns=("raylet", "gcs_server", "plasma")):
    rc, out, _ = _sh(["srun", "-N1", "-n1", "--overlap", "--nodelist", node, "pgrep", "-af",
                      "|".join(patterns)], timeout=60, env=env)
    if rc not in (0, 1):
        rc, out, _ = _sh(["srun", "-N1", "-n1", "--overlap", "--nodelist", node, "bash", "-lc",
                          "ps -e -o comm= | grep -E 'raylet|gcs_server|plasma' || true"],
                         timeout=60, env=env)
    # Drop the detector's own srun/pgrep argv (it contains the pattern string); real ray daemons never
    # carry srun/pgrep in argv.
    return [ln for ln in (out or "").splitlines()
            if ln.strip() and "pgrep" not in ln and "srun" not in ln]


def _wait_tcp(ip, port, timeout_s=90):
    import socket
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        s = socket.socket()
        s.settimeout(2)
        try:
            s.connect((ip, int(port)))
            s.close()
            return True
        except Exception:  # noqa: BLE001
            try:
                s.close()
            except Exception:  # noqa: BLE001
                pass
            time.sleep(1.5)
    raise RuntimeError(f"GCS {ip}:{port} not reachable within {timeout_s}s")


def _wait_ray_nodes(ray, expected, timeout_s=120):
    deadline = time.time() + timeout_s
    alive = 0
    while time.time() < deadline:
        alive = len([nd for nd in ray.nodes() if nd.get("Alive")])
        if alive >= expected:
            return True
        time.sleep(1.5)
    raise RuntimeError(f"only {alive} Ray nodes Alive (<{expected}) within {timeout_s}s")


def _ray_node_id_for_ip(ray, ip):
    for nd in ray.nodes():
        if nd.get("Alive") and nd.get("NodeManagerAddress") == ip:
            return nd.get("NodeID")
    return None


def compute_ray_payload_gates(*, x, n, payload_bytes, remote_node_ids, coordinator_node_id,
                              driver_node_id, head_num_cpus, coordinator_num_cpus, hard_placement,
                              records, measured_composite, folded_digest, no_dispatch_timeout):
    """PURE per-call gate booleans for ONE Ray payload-coordinator result. `records` is a list of
    {i, value, payload (bytes), node_id}. Placement is by RAY NODE ID (strings): coordinator on the
    head/driver node running ZERO leaves, N leaves hard-pinned across the remote worker nodes (balanced
    round-robin), all leaves remote. Payload gates check the byte length crossing the boundary and the
    post-timing folded digest. All gates must be True to pass."""
    node_ids = [r["node_id"] for r in records]
    per_node = {}
    for nid in node_ids:
        per_node[nid] = per_node.get(nid, 0) + 1
    leaves_local = sum(1 for nid in node_ids if nid == coordinator_node_id)
    leaves_remote = sum(1 for nid in node_ids if nid != coordinator_node_id)
    covered = set(node_ids)
    r = len(remote_node_ids)
    expected_each = sorted(n // r + (1 if j < n % r else 0) for j in range(r)) if r else []
    got_each = sorted(per_node.get(rid, 0) for rid in remote_node_ids)
    return {
        "coordinator_on_head_node": (coordinator_node_id is not None
                                     and coordinator_node_id == driver_node_id),
        "coordinator_num_cpus_zero": coordinator_num_cpus == 0,
        "ray_head_num_cpus_zero": head_num_cpus == 0,
        "coordinator_runs_zero_leaves": per_node.get(coordinator_node_id, 0) == 0,
        "hard_placement": hard_placement is True,
        "n_leaves_dispatched": len(records) == n,
        "witness_leaf_count_n": len(records) == n,
        "leaves_local_zero": leaves_local == 0,
        "leaves_remote_all": leaves_remote == n,
        "every_remote_node_covered": bool(remote_node_ids)
        and all(rid in covered for rid in remote_node_ids),
        "each_remote_node_ge1": bool(remote_node_ids)
        and all(per_node.get(rid, 0) >= 1 for rid in remote_node_ids),
        "leaves_per_remote_balanced": bool(remote_node_ids) and got_each == expected_each,
        "scalar_oracle_correct": int(measured_composite) == composite_oracle(x, n),
        "payload_byte_length_correct": all(len(rr["payload"]) == payload_bytes for rr in records),
        "payload_digest_correct": folded_digest == payload_digest(x, n, payload_bytes),
        "no_dispatch_timeout": no_dispatch_timeout is True,
    }


def run_ray_payload_smoke(*, x=7, n=8, sizes=(0, 262144), env=None, import_fn=None, write=True,
                          prefer_subnet="10.42.5.", n_remote=2, ray_port=6379, worker_cpus=8,
                          ray_dispatch_timeout_s=30.0, prewarm=3, measured=5):
    """Ray matched payload smoke over `sizes` (S=0 first). Bootstraps a (1 + n_remote)-node Ray cluster
    (head num_cpus=0 on node A + workers), pins a coordinator to node A (num_cpus=0, ZERO leaves) that
    hard-pins N leaves round-robin across the remote node ids, times one ray.get(coordinator.remote(...))
    per iteration returning payload bytes, folds the digest AFTER timing, writes per-size gitignored ray
    artifacts, and tears the cluster down with an orphan check. Skips cleanly off-cluster / without Ray.
    Larger sizes are not dispatched once an earlier size fails."""
    env = dict(os.environ if env is None else env)
    slurm = _slurm_info(env)
    hostnames = slurm["hostnames"]
    if len(hostnames) < (1 + n_remote):
        print(f"exp64 ray-payload-remote-smoke: SKIP -- need >=1+{n_remote} nodes "
              f"(hostnames={hostnames}).")
        return 0
    try:
        ray = (import_fn or _default_ray_import)()
    except ImportError as exc:
        print(f"exp64 ray-payload-remote-smoke: SKIP -- ray not installed ({exc}).")
        return 0

    os.makedirs(EXP64_RUNS, exist_ok=True)
    node_a = hostnames[0]
    remote_nodes = hostnames[1:1 + n_remote]
    node_set = [_short_host(node_a)] + sorted(_short_host(h) for h in remote_nodes)
    ip_a = _node_ip_on_subnet(node_a, prefer_subnet, env)
    remote_ips = [_node_ip_on_subnet(h, prefer_subnet, env) for h in remote_nodes]
    head_log = os.path.join(EXP64_RUNS, f"ray_head_{slurm['slurm_job_id']}.log")

    head_p, worker_ps = None, []
    error, no_orphans = None, None
    per_size = []          # (S, calls, dispatch_timed_out)
    failed_before = None
    head_num_cpus, coord_num_cpus = 0, 0
    nid_a, remote_nids, ray_version = None, [], None
    resource_map, driver_cpuset = None, None
    try:
        if not ip_a or any(ip is None for ip in remote_ips):
            raise RuntimeError(f"could not resolve subnet IPs (a={ip_a} remotes={remote_ips})")
        head_cmd = ["srun", "-N1", "-n1", "--overlap", "--nodelist", node_a, "--export=ALL",
                    "ray", "start", "--head", "--node-ip-address", ip_a, "--port", str(ray_port),
                    "--include-dashboard", "false", "--num-cpus", str(head_num_cpus), "--block"]
        head_p = _ray_popen_block(head_cmd, env, head_log)
        _wait_tcp(ip_a, ray_port, timeout_s=90)
        for h, ip_h in zip(remote_nodes, remote_ips):
            wlog = os.path.join(EXP64_RUNS, f"ray_worker_{_short_host(h)}_{slurm['slurm_job_id']}.log")
            wcmd = ["srun", "-N1", "-n1", "--overlap", "--nodelist", h, "--export=ALL",
                    "ray", "start", "--address", f"{ip_a}:{ray_port}", "--node-ip-address", ip_h,
                    "--num-cpus", str(worker_cpus), "--block"]
            worker_ps.append(_ray_popen_block(wcmd, env, wlog))
        ray.init(address=f"{ip_a}:{ray_port}", log_to_driver=False)
        _wait_ray_nodes(ray, expected=1 + n_remote, timeout_s=120)
        ray_version = getattr(ray, "__version__", None)
        try:
            resource_map = dict(ray.cluster_resources())
        except Exception:  # noqa: BLE001
            resource_map = None
        driver_cpuset = _effective_cpuset()  # driver runs on the head node under --cpu-bind=none
        nid_a = _ray_node_id_for_ip(ray, ip_a)
        remote_nids = [_ray_node_id_for_ip(ray, ip) for ip in remote_ips]
        if not nid_a or any(nid is None for nid in remote_nids) \
                or len(set([nid_a] + remote_nids)) != (1 + n_remote):
            raise RuntimeError(f"could not resolve distinct Ray node ids "
                               f"(a={nid_a} remotes={remote_nids})")

        from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
        from ray.exceptions import GetTimeoutError
        mask64 = MASK64
        leaf_xor = LEAF_XOR

        @ray.remote
        def _leaf(xx, i, s):
            # Inlined closed-int64 leaf + synthetic payload sawtooth (self-contained; matches the
            # module oracle so no dependence on importing this module on the worker).
            v = (((int(xx) & mask64) ^ leaf_xor) + ((int(i) & mask64) << 1)) & mask64
            val = v - (1 << 64) if v >= (1 << 63) else v
            base = val & mask64
            payload = bytes((base + k) & 0xFF for k in range(int(s)))
            return (int(i), int(val), payload, str(ray.get_runtime_context().get_node_id()))

        @ray.remote
        def _coordinator(xx, nn, s, target_nids):
            r = len(target_nids)
            futs = []
            for i in range(nn):
                strat = NodeAffinitySchedulingStrategy(node_id=target_nids[i % r], soft=False)
                futs.append(_leaf.options(scheduling_strategy=strat).remote(xx, i, s))
            recs = ray.get(futs)  # coordinator gathers; it does NOT fold the payload digest
            acc = 0
            for rr in recs:
                acc = (acc + (int(rr[1]) & mask64)) & mask64
            composite = acc - (1 << 64) if acc >= (1 << 63) else acc
            return (int(composite), recs, str(ray.get_runtime_context().get_node_id()))

        coord_strat = NodeAffinitySchedulingStrategy(node_id=nid_a, soft=False)

        def _submit(s):
            return ray.get(_coordinator.options(num_cpus=coord_num_cpus,
                                                scheduling_strategy=coord_strat)
                           .remote(x, n, s, remote_nids), timeout=ray_dispatch_timeout_s)

        for s in sizes:
            s = int(s)
            calls, dispatch_timed_out = [], False
            try:
                for _ in range(prewarm):
                    _submit(s)
                for c in range(measured):
                    t0 = time.monotonic_ns()
                    composite, recs, coord_node = _submit(s)
                    t1 = time.monotonic_ns()  # RTT boundary: payload bytes are back in Python
                    records = [{"i": int(i), "value": int(v), "payload": bytes(pay),
                                "node_id": str(nid)} for (i, v, pay, nid) in recs]
                    d0 = time.monotonic_ns()
                    folded = fold_payload_digest(records)  # fold AFTER timing, OUTSIDE the RTT window
                    d1 = time.monotonic_ns()
                    per_node = {}
                    for rr in records:
                        per_node[rr["node_id"]] = per_node.get(rr["node_id"], 0) + 1
                    gates = compute_ray_payload_gates(
                        x=x, n=n, payload_bytes=s, remote_node_ids=remote_nids,
                        coordinator_node_id=coord_node, driver_node_id=nid_a,
                        head_num_cpus=head_num_cpus, coordinator_num_cpus=coord_num_cpus,
                        hard_placement=True, records=records, measured_composite=composite,
                        folded_digest=folded, no_dispatch_timeout=True)
                    calls.append({
                        "call_index": c, "rtt_ns": t1 - t0, "rtt_ms": (t1 - t0) / 1e6,
                        "digest_check_ns": d1 - d0, "digest_check_ms": (d1 - d0) / 1e6,
                        "folded_digest": folded, "expected_digest": payload_digest(x, n, s),
                        "composite": int(composite), "coordinator_node_id": coord_node,
                        "leaves_per_node": per_node, "gates": gates, "gates_pass": all(gates.values()),
                    })
            except GetTimeoutError:
                dispatch_timed_out = True
                print(f"exp64 ray-payload-remote-smoke: S={s} GetTimeoutError after "
                      f"{ray_dispatch_timeout_s}s -- failing closed.")
            per_size.append((s, calls, dispatch_timed_out))
            if dispatch_timed_out or not (calls and all(cc["gates_pass"] for cc in calls)):
                failed_before = s  # do not dispatch any larger S in this cluster
                break
    except Exception as exc:  # noqa: BLE001
        error = f"{type(exc).__name__}: {exc}"
        print(f"exp64 ray-payload-remote-smoke: ERROR {error}")
    finally:
        try:
            ray.shutdown()
        except Exception:  # noqa: BLE001
            pass
        for h in remote_nodes:
            _ray_stop_node(h, env)
        _ray_stop_node(node_a, env)
        for p in ([*worker_ps, head_p]):
            if p is not None:
                try:
                    p.terminate()
                    p.wait(timeout=20)
                except Exception:  # noqa: BLE001
                    p.kill()
                try:
                    p._exp64_logfile.close()
                except Exception:  # noqa: BLE001
                    pass
        orph = _orphan_check_node(node_a, env)
        for h in remote_nodes:
            orph += _orphan_check_node(h, env)
        no_orphans = (len(orph) == 0)
        print(f"exp64 ray-payload-remote-smoke: teardown no_orphans={no_orphans}"
              + (f" orphans={orph}" if orph else ""))

    fences_ok, _ = validate_provenance(EXP64_DESIGN)
    rc = 0
    for s, calls, dispatch_timed_out in per_size:
        all_calls_pass = bool(calls) and all(c["gates_pass"] for c in calls)
        all_remote_all_calls = bool(calls) and all(
            c["gates"].get("leaves_local_zero") is True and c["gates"].get("leaves_remote_all") is True
            for c in calls)
        distribution_balanced_all_calls = bool(calls) and all(
            c["gates"].get("leaves_per_remote_balanced") is True for c in calls)
        dispatch_no_timeout_all_calls = (not dispatch_timed_out) and bool(calls) and all(
            c["gates"].get("no_dispatch_timeout") is True for c in calls)
        structural_gates = {
            "all_calls_gates_pass": all_calls_pass,
            "no_dispatch_timeout": not dispatch_timed_out,
            "no_orphan_ray_processes": bool(no_orphans),
            "coordinator_on_head_num_cpus_zero": True,
            "timed_call_returns_payload_bytes_to_python": True,
            "digest_folded_inside_runtime": False,
            "digest_check_after_timing_outside_rtt": True,
            "fences_locked_false": fences_ok,
            "same_axis_comparison_false": EXP64_DESIGN["same_axis_comparison"] is False,
        }
        overall_pass = (error is None and all_calls_pass and not dispatch_timed_out
                        and bool(no_orphans))
        rec = build_provenance(x=x, n=n, sizes=[s], phase="ray-payload-remote-smoke")
        rec.update({
            "kind": "ray_payload_remote_smoke", "arm": "ray", "slice": 2,
            "payload_bytes": s,
            "payload_transport": "ray_object_transport",
            "composition_primitive": "ray.coordinator.remote",
            "reduce_primitive": "coordinator_fold_sum_int64",
            "payload_is_synthetic": True, "payload_not_model_output": True, "no_inference": True,
            "n_remote": n_remote, "prewarm": prewarm, "measured": measured,
            "ray_port": ray_port, "worker_cpus": worker_cpus,
            "ray_dispatch_timeout_s": ray_dispatch_timeout_s, "prefer_subnet": prefer_subnet,
            "selected_subnet": prefer_subnet, "head_ip": ip_a,
            "ray_version": ray_version,
            "driver_node_id": nid_a, "remote_node_ids": remote_nids,
            "ray_head_num_cpus": head_num_cpus, "ray_coordinator_num_cpus": coord_num_cpus,
            "hard_placement": True, "soft": False,
            "node_set": node_set,
            "expected_digest": payload_digest(x, n, s),
            "calls": calls, "structural_gates": structural_gates,
            "overall_pass": overall_pass, "dispatch_timed_out": dispatch_timed_out,
            "no_orphans": no_orphans, "error": error,
            "slurm_job_id": slurm["slurm_job_id"],

            # timing boundary + evidence grade (folded from HPX/runtime review)
            "boundary": TIMING_BOUNDARY,
            "clock": TIMING_CLOCK,
            "evidence_grade": EVIDENCE_GRADE_R1,
            "distributional_evidence": False,
            "percentiles_evidence_ready": False,

            # Ray transport/placement OBSERVATIONS (recorded provenance, not tuned/equalized)
            "transport_family": "ray_object_transport",
            "object_return_path": "not_observed",   # inline-vs-plasma not exposed by public Ray API here
            "plasma_engagement": "not_observed",
            "resource_map": resource_map if resource_map is not None else "not_observed",
            "cpu_bind_mode": "none",
            "cpu_bind_note": "ray driver step launched with --cpu-bind=none (Slice 2 GCS-starvation fix)",
            "effective_cpu_binding": driver_cpuset,
            "phase_affinity_recorded": driver_cpuset is not None,

            # cross-phase manifest evidence
            "no_orphan_proof": bool(no_orphans),
            "prewarm_excluded_from_timed": True,
            "all_remote_all_calls": all_remote_all_calls,
            "distribution_balanced_all_calls": distribution_balanced_all_calls,
            "dispatch_no_timeout_all_calls": dispatch_no_timeout_all_calls,
        })
        if write:
            path = os.path.join(EXP64_RUNS,
                                f"exp64_payload_smoke_{slurm['slurm_job_id']}_S{s}_ray.json")
            with open(path, "w") as f:
                json.dump(rec, f, indent=2)
        print(f"exp64 ray-payload-smoke S={s}: overall_pass={overall_pass} error={error} "
              f"calls={len(calls)} mean_rtt_ms={_mean_rtt_ms(rec):.3f} "
              f"remote_node_ids={remote_nids}")
        if write:
            print(f"  wrote exp64_payload_smoke_{slurm['slurm_job_id']}_S{s}_ray.json (gitignored)")
        if not overall_pass:
            rc = 1
    if failed_before is not None:
        print(f"exp64 ray-payload-smoke: S={failed_before} did NOT pass; larger sizes not dispatched.")
    return rc


# ---------------------------------------------------------------------------
# Slice 3 payload-ladder MANIFEST (PURE pairing/aggregation -- no runtime, no cluster). It pairs the
# 5 HPX + 5 Ray per-size artifacts of ONE allocation by payload_bytes and checks STRUCTURAL correlation
# gates only. It computes NO cross-arm arithmetic: no ratios, no differences, no speedups, no winner.
# Per-arm data is stored in separate keyed blocks (arms.hpx / arms.ray), never parallel columns. R=1 is
# structural machinery validation; same_axis_comparison is a structural-correlation flag the manifest
# may set True ONLY if every gate passes, and even then it licenses no distributional/evidentiary claim.
# ---------------------------------------------------------------------------

def _scan_forbidden_keys(obj, found):
    """Recursively collect any dict keys containing a forbidden speedup/superiority substring."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            low = str(k).lower()
            for bad in FORBIDDEN_KEY_SUBSTRINGS:
                if bad in low:
                    found.append(k)
            _scan_forbidden_keys(v, found)
    elif isinstance(obj, list):
        for v in obj:
            _scan_forbidden_keys(v, found)
    return found


def _rtt_within_arm_summary(art):
    """WITHIN-ARM RTT observation for one size (mean/min/max ms). Never a cross-arm operand."""
    calls = art.get("calls", [])
    rtts = [c.get("rtt_ms") for c in calls if isinstance(c.get("rtt_ms"), (int, float))]
    if not rtts:
        return {"n_calls": len(calls), "mean_rtt_ms": None, "min_rtt_ms": None,
                "max_rtt_ms": None, "note": WITHIN_ARM_NOTE}
    return {"n_calls": len(rtts), "mean_rtt_ms": sum(rtts) / len(rtts),
            "min_rtt_ms": min(rtts), "max_rtt_ms": max(rtts), "note": WITHIN_ARM_NOTE}


def _index_by_size(arts):
    return {int(a.get("payload_bytes")): a for a in arts if a.get("payload_bytes") is not None}


_HPX_PROV_KEYS = ("hpx_composition", "hpx_composition_note", "hpx_parcelport", "transport_family",
                  "tcp_nodelay", "serialize_buffer_mode", "zero_copy_optimization",
                  "array_optimization", "coalescing", "size_thresholds", "connector_lifecycle_ok",
                  "serve_timeout_s", "dispatch_timeout_s", "root_ip", "hpx_teardown_clean",
                  # Slice 4 review-folded provenance blocks (echoed into the manifest for the band)
                  "hpx_poll", "hpx_runtime", "hpx_serialization", "numa_nic",
                  "connector_anomaly_witness")
_RAY_PROV_KEYS = ("ray_version", "driver_node_id", "remote_node_ids", "resource_map",
                  "ray_head_num_cpus", "ray_coordinator_num_cpus", "hard_placement", "soft",
                  "worker_cpus", "transport_family", "object_return_path", "plasma_engagement",
                  "cpu_bind_mode", "cpu_bind_note", "no_orphan_proof", "ray_dispatch_timeout_s",
                  "head_ip")


def _arm_provenance(arts, *, arm):
    a0 = arts[0] if arts else {}
    prov = {
        "arm": arm,
        "slurm_job_id": a0.get("slurm_job_id"),
        "node_set": a0.get("node_set"),
        "n": a0.get("n"),
        "prewarm": a0.get("prewarm"),
        "measured": a0.get("measured"),
        "boundary": a0.get("boundary"),
        "clock": a0.get("clock"),
        "prefer_subnet": a0.get("prefer_subnet"),
        "selected_subnet": a0.get("selected_subnet"),
        "evidence_grade": a0.get("evidence_grade"),
        "phase_affinity_recorded": a0.get("phase_affinity_recorded"),
        "effective_cpu_binding": a0.get("effective_cpu_binding"),
        "prewarm_excluded_from_timed": a0.get("prewarm_excluded_from_timed"),
    }
    keys = _HPX_PROV_KEYS if arm == "hpx" else _RAY_PROV_KEYS
    for k in keys:
        if k in a0:
            prov[k] = a0.get(k)
    return prov


def _manifest_correlation_gates(hpx_arts, ray_arts, hpx_by, ray_by, *, job, ladder,
                                required_measured=None):
    """Structural correlation gates over the two arms' per-size artifacts. Every value must be True for
    the manifest to earn same_axis_comparison. No numeric cross-arm comparison is performed here.

    required_measured (Slice 4 band use) enforces measured >= required in BOTH arms at every size; when
    None (Slice 3 use) the gate passes trivially so the R=1 manifest behavior is unchanged."""
    hpx0 = hpx_arts[0] if hpx_arts else {}
    ray0 = ray_arts[0] if ray_arts else {}
    hpx_sizes, ray_sizes, ladder_set = set(hpx_by), set(ray_by), set(int(s) for s in ladder)

    def _all(pred, by):
        return bool(by) and all(pred(by[s]) for s in by)

    def _node_set(a):
        return tuple(sorted(a.get("node_set") or []))

    def _sg(a, key):
        return a.get("structural_gates", {}).get(key)

    # expected_digest must equal the closed oracle in BOTH arms at every paired size (same x,n).
    oracle_ok = bool(hpx_sizes & ray_sizes)
    for s in (hpx_sizes & ray_sizes):
        h, r = hpx_by[s], ray_by[s]
        try:
            oracle = payload_digest(int(h.get("x")), int(h.get("n")), int(s))
        except Exception:  # noqa: BLE001
            oracle_ok = False
            continue
        if not (h.get("expected_digest") == oracle and r.get("expected_digest") == oracle
                and h.get("x") == r.get("x") and h.get("n") == r.get("n")):
            oracle_ok = False

    forbidden = _scan_forbidden_keys({"hpx": hpx_arts, "ray": ray_arts}, [])

    def _fences_false(a):
        return all(a.get(k) is False for k in FENCE_KEYS_FALSE)

    def _on_subnet(a):
        return str(a.get("selected_subnet") or "").startswith("10.42.5.")

    return {
        "single_slurm_job_identity": (str(hpx0.get("slurm_job_id")) == str(job)
                                      and str(ray0.get("slurm_job_id")) == str(job)
                                      and str(job) not in ("", "None")),
        "node_set_matched": bool(_node_set(hpx0)) and _node_set(hpx0) == _node_set(ray0),
        "n_matched": hpx0.get("n") is not None and hpx0.get("n") == ray0.get("n"),
        "ladder_fully_covered_both_arms": hpx_sizes == ladder_set and ray_sizes == ladder_set,
        "prewarm_matched": hpx0.get("prewarm") is not None and hpx0.get("prewarm") == ray0.get("prewarm"),
        "measured_matched": (hpx0.get("measured") is not None
                             and hpx0.get("measured") == ray0.get("measured")),
        "boundary_matched": (hpx0.get("boundary") == TIMING_BOUNDARY
                             and ray0.get("boundary") == TIMING_BOUNDARY),
        "clock_matched": hpx0.get("clock") == TIMING_CLOCK and ray0.get("clock") == TIMING_CLOCK,
        "subnet_matched": (hpx0.get("prefer_subnet") is not None
                           and hpx0.get("prefer_subnet") == ray0.get("prefer_subnet")
                           and hpx0.get("selected_subnet") == ray0.get("selected_subnet")),
        "transport_family_hpx_tcp_on_subnet": hpx0.get("hpx_parcelport") == "tcp" and _on_subnet(hpx0),
        "transport_family_ray_on_subnet": _on_subnet(ray0),
        "both_arms_all_remote_all_sizes": (_all(lambda a: a.get("all_remote_all_calls") is True, hpx_by)
                                           and _all(lambda a: a.get("all_remote_all_calls") is True, ray_by)),
        "both_arms_balanced_distribution_all_sizes":
            (_all(lambda a: a.get("distribution_balanced_all_calls") is True, hpx_by)
             and _all(lambda a: a.get("distribution_balanced_all_calls") is True, ray_by)),
        "payload_bytes_cross_boundary_both_arms":
            (_all(lambda a: _sg(a, "timed_call_returns_payload_bytes_to_python") is True, hpx_by)
             and _all(lambda a: _sg(a, "timed_call_returns_payload_bytes_to_python") is True, ray_by)),
        "digest_folded_after_timing_both_arms":
            (_all(lambda a: _sg(a, "digest_check_after_timing_outside_rtt") is True
                  and _sg(a, "digest_folded_inside_runtime") is False, hpx_by)
             and _all(lambda a: _sg(a, "digest_check_after_timing_outside_rtt") is True
                      and _sg(a, "digest_folded_inside_runtime") is False, ray_by)),
        "expected_digest_matched_every_size": oracle_ok and (hpx_sizes & ray_sizes) == ladder_set,
        "both_arms_overall_pass_all_sizes": (_all(lambda a: a.get("overall_pass") is True, hpx_by)
                                             and _all(lambda a: a.get("overall_pass") is True, ray_by)),
        "no_dispatch_timeout_both_arms":
            (_all(lambda a: a.get("dispatch_no_timeout_all_calls") is True, hpx_by)
             and _all(lambda a: a.get("dispatch_no_timeout_all_calls") is True, ray_by)),
        "ray_no_orphan_proof": _all(lambda a: a.get("no_orphan_proof") is True, ray_by),
        "hpx_residue_clear_before_ray": _all(lambda a: a.get("hpx_teardown_clean") is True, hpx_by),
        "hpx_phase_affinity_recorded": _all(lambda a: a.get("phase_affinity_recorded") is True, hpx_by),
        "ray_phase_affinity_recorded": _all(lambda a: a.get("phase_affinity_recorded") is True, ray_by),
        "prewarm_excluded_from_timed_both_arms":
            (_all(lambda a: a.get("prewarm_excluded_from_timed") is True
                  and len(a.get("calls", [])) == a.get("measured"), hpx_by)
             and _all(lambda a: a.get("prewarm_excluded_from_timed") is True
                      and len(a.get("calls", [])) == a.get("measured"), ray_by)),
        "evidence_grade_structural_r1_both_arms":
            (_all(lambda a: a.get("evidence_grade") == EVIDENCE_GRADE_R1, hpx_by)
             and _all(lambda a: a.get("evidence_grade") == EVIDENCE_GRADE_R1, ray_by)),
        "measured_ge_required_both_arms":
            (required_measured is None)
            or (_all(lambda a: a.get("measured") is not None and a.get("measured") >= required_measured
                     and len(a.get("calls", [])) == a.get("measured"), hpx_by)
                and _all(lambda a: a.get("measured") is not None and a.get("measured") >= required_measured
                         and len(a.get("calls", [])) == a.get("measured"), ray_by)),
        "all_fences_false": _all(_fences_false, hpx_by) and _all(_fences_false, ray_by),
        "no_forbidden_keys": len(forbidden) == 0,
    }


def build_payload_ladder_manifest(hpx_arts, ray_arts, *, job, ladder_sizes=DEFAULT_SIZE_LADDER, r=1,
                                  band_id=None, island_index=None, required_measured=None):
    """PURE builder: pair the HPX + Ray per-size artifacts of one allocation, store per-arm keyed blocks,
    compute structural correlation gates, and set same_axis_comparison = (all gates pass). No cross-arm
    arithmetic is computed. On ANY gate failure same_axis_comparison and overall_manifest_pass are False.

    band_id/island_index/required_measured are optional Slice 4 fields; when present the manifest also
    gates measured >= required. They do not change the R=1 (Slice 3) evidence grade."""
    ladder = [int(s) for s in ladder_sizes]
    hpx_by, ray_by = _index_by_size(hpx_arts), _index_by_size(ray_arts)
    sizes_paired = sorted(set(hpx_by) & set(ray_by))
    hpx_prov, ray_prov = _arm_provenance(hpx_arts, arm="hpx"), _arm_provenance(ray_arts, arm="ray")

    def _by_size_block(by):
        return {str(s): {"payload_bytes": s, "rtt_within_arm": _rtt_within_arm_summary(by[s]),
                         "expected_digest": by[s].get("expected_digest"),
                         "overall_pass": bool(by[s].get("overall_pass"))}
                for s in sorted(by)}

    arms = {
        "hpx": {"provenance": hpx_prov, "by_size": _by_size_block(hpx_by)},
        "ray": {"provenance": ray_prov, "by_size": _by_size_block(ray_by)},
    }
    gates = _manifest_correlation_gates(hpx_arts, ray_arts, hpx_by, ray_by, job=str(job), ladder=ladder,
                                        required_measured=required_measured)
    gates_all_pass = bool(gates) and all(gates.values())

    manifest = {
        "experiment": "exp64",
        "kind": "payload_ladder_manifest",
        "phase": "payload-ladder-manifest",
        "slice": 3,
        "title": "exp64 Slice 3 matched payload ladder structural manifest (R=1)",
        "job": str(job),
        "band_id": band_id,
        "island_index": island_index,
        "required_measured": required_measured,
        "ladder_sizes": ladder,
        "sizes_paired": sizes_paired,
        "r": int(r),
        "prewarm": hpx_prov.get("prewarm"),
        "measured": hpx_prov.get("measured"),
        "evidence_grade": EVIDENCE_GRADE_R1,
        "distributional_evidence": False,
        "percentiles_evidence_ready": False,
        "honesty_notes": dict(MANIFEST_HONESTY_NOTES),
        "arms": arms,
        "correlation_gates": gates,
        "gates_all_pass": gates_all_pass,
        # structural-correlation flag ONLY; never a distributional/evidentiary claim
        "same_axis_comparison": bool(gates_all_pass),
        "no_cross_arm_timing_computed": True,
        "arms_differenced": False,
        "ratio_reported": False,
        "speedup_computed": False,
        "placement_bands_differenced": False,
        "overall_manifest_pass": bool(gates_all_pass),
        "created_monotonic_ns": time.monotonic_ns(),
    }
    return manifest


def validate_payload_ladder_manifest(manifest):
    """Fail-closed structural verifier for a payload-ladder manifest. Returns (ok, problems)."""
    problems = []
    for k in MANIFEST_FENCE_KEYS_FALSE:
        if manifest.get(k) is not False:
            problems.append(f"manifest fence {k} must be False, got {manifest.get(k)!r}")
    if manifest.get("no_cross_arm_timing_computed") is not True:
        problems.append("no_cross_arm_timing_computed must be True")
    if manifest.get("evidence_grade") != EVIDENCE_GRADE_R1:
        problems.append("evidence_grade must be structural_r1")
    found = _scan_forbidden_keys(manifest, [])
    if found:
        problems.append(f"forbidden claim keys present: {found}")
    gates = manifest.get("correlation_gates", {})
    gates_all = bool(gates) and all(gates.values())
    if manifest.get("gates_all_pass") != gates_all:
        problems.append("gates_all_pass inconsistent with correlation_gates")
    # same_axis_comparison / overall_manifest_pass may be True ONLY when every gate passes
    if manifest.get("same_axis_comparison") and not gates_all:
        problems.append("same_axis_comparison True while a correlation gate fails")
    if manifest.get("overall_manifest_pass") and not gates_all:
        problems.append("overall_manifest_pass True while a correlation gate fails")
    if not gates_all:
        if manifest.get("same_axis_comparison") is not False:
            problems.append("same_axis_comparison must be False on any gate failure")
        if manifest.get("overall_manifest_pass") is not False:
            problems.append("overall_manifest_pass must be False on any gate failure")
    return (len(problems) == 0, problems)


def run_payload_ladder_manifest(*, job, ladder_sizes=DEFAULT_SIZE_LADDER, runs_dir=EXP64_RUNS,
                                write=True, band_id=None, island_index=None, required_measured=None):
    """Glob the per-size HPX + Ray artifacts for `job`, build + validate the manifest, write it. Skips
    cleanly (rc=0) when no paired artifacts exist for the job (off-cluster / not yet run)."""
    import glob
    if not job:
        print("exp64 payload-ladder-manifest: SKIP -- no --job provided.")
        return 0
    hpx_paths = sorted(glob.glob(os.path.join(runs_dir, f"exp64_payload_smoke_{job}_S*_hpx.json")))
    ray_paths = sorted(glob.glob(os.path.join(runs_dir, f"exp64_payload_smoke_{job}_S*_ray.json")))
    hpx_arts = [a for a in (_read_json(p) for p in hpx_paths) if a]
    ray_arts = [a for a in (_read_json(p) for p in ray_paths) if a]
    if not hpx_arts or not ray_arts:
        print(f"exp64 payload-ladder-manifest: SKIP -- no paired artifacts for job={job} "
              f"(hpx={len(hpx_arts)} ray={len(ray_arts)}) under {runs_dir}.")
        return 0
    manifest = build_payload_ladder_manifest(hpx_arts, ray_arts, job=job, ladder_sizes=ladder_sizes,
                                             band_id=band_id, island_index=island_index,
                                             required_measured=required_measured)
    ok, problems = validate_payload_ladder_manifest(manifest)
    manifest["validator_ok"] = ok
    manifest["validator_problems"] = problems
    if write:
        path = os.path.join(runs_dir, f"exp64_payload_ladder_{job}_manifest.json")
        with open(path, "w") as f:
            json.dump(manifest, f, indent=2)
    failed = [g for g, v in manifest["correlation_gates"].items() if not v]
    print(f"exp64 payload-ladder-manifest job={job}: "
          f"overall_manifest_pass={manifest['overall_manifest_pass']} "
          f"same_axis_comparison={manifest['same_axis_comparison']} validator_ok={ok} "
          f"sizes_paired={manifest['sizes_paired']} gates_failed={failed or 'none'}")
    if problems:
        print(f"  validator_problems={problems}")
    if write:
        print(f"  wrote exp64_payload_ladder_{job}_manifest.json (gitignored)")
    return 0 if (manifest["overall_manifest_pass"] and ok) else 1


# ---------------------------------------------------------------------------
# Slice 4 payload-ladder BAND aggregate (PURE; R matched islands). It reads R per-island manifests +
# their per-size arm JSONs, computes WITHIN-ARM distributions (p50/p90/min/mean/max/CV + coarse
# variability flags) per arm/size/island and across-island median/range per arm/size, and earns
# matched_band_r5 only if every band gate passes. It computes NO cross-arm arithmetic and NEVER mixes
# an HPX value with a Ray value in any expression. A stronger distributional_payload_ladder grade stays
# BLOCKED because the HPX serialization runtime (zero-copy) path is not observed.
# ---------------------------------------------------------------------------

def _percentile(sorted_vals, q):
    """Linear-interpolated percentile on a pre-sorted list; q in [0,1]. None on empty."""
    if not sorted_vals:
        return None
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    pos = q * (len(sorted_vals) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(sorted_vals) - 1)
    return sorted_vals[lo] * (1 - (pos - lo)) + sorted_vals[hi] * (pos - lo)


def _within_arm_stats(values):
    """PURE within-arm RTT stats for ONE arm's samples. Receives a single arm's numbers only -- never
    both arms. p50/p90/min/mean/max/CV plus coarse, deterministic variability flags."""
    vals = sorted(float(v) for v in values if isinstance(v, (int, float)))
    n = len(vals)
    if n == 0:
        return {"n": 0, "p50_ms": None, "p90_ms": None, "min_ms": None, "mean_ms": None,
                "max_ms": None, "cv": None, "high_variability_flag": False,
                "multimodal_suspected": False, "note": WITHIN_ARM_NOTE}
    mean = sum(vals) / n
    std = (sum((v - mean) ** 2 for v in vals) / n) ** 0.5
    cv = (std / mean) if mean else None
    p50, p90, mn, mx = _percentile(vals, 0.5), _percentile(vals, 0.9), vals[0], vals[-1]
    lower_spread = max(p50 - mn, 1e-9)
    multimodal = (p90 - p50) > MULTIMODAL_TAIL_RATIO * lower_spread
    return {"n": n, "p50_ms": p50, "p90_ms": p90, "min_ms": mn, "mean_ms": mean, "max_ms": mx,
            "cv": cv, "high_variability_flag": bool(cv is not None and cv > CV_HIGH_THRESHOLD),
            "multimodal_suspected": bool(multimodal), "note": WITHIN_ARM_NOTE}


def _across_island(per_island_vals):
    """PURE across-island median + range for ONE arm's per-island percentile values (never cross-arm)."""
    vals = sorted(float(v) for v in per_island_vals if isinstance(v, (int, float)))
    if not vals:
        return {"median": None, "range": [None, None], "n_islands": 0,
                "spread_kind": "across_island_range", "note": WITHIN_ARM_NOTE}
    return {"median": _percentile(vals, 0.5), "range": [vals[0], vals[-1]], "n_islands": len(vals),
            "spread_kind": "across_island_range", "note": WITHIN_ARM_NOTE}


_CROSS_ARM_TOKENS = ("rtt_diff", "rtt_ratio", "_delta_", "delta_ms", "cross_arm_ratio", "arm_ratio",
                     "vs_ray", "vs_hpx", "ratio_value", "speedup_value")


def _scan_cross_arm_tokens(obj, found):
    if isinstance(obj, dict):
        for k, v in obj.items():
            low = str(k).lower()
            for t in _CROSS_ARM_TOKENS:
                if t in low:
                    found.append(k)
            _scan_cross_arm_tokens(v, found)
    elif isinstance(obj, list):
        for v in obj:
            _scan_cross_arm_tokens(v, found)
    return found


def _island_quality(island, *, ladder, required_measured):
    """Per-island quality: 'clean' or 'flagged' with reasons. Flagged islands fail the band closed --
    the band is never silently cherry-picked."""
    reasons = []
    m = island.get("manifest", {})
    if m.get("overall_manifest_pass") is not True:
        reasons.append("manifest_not_pass")
    if m.get("same_axis_comparison") is not True:
        reasons.append("manifest_same_axis_false")
    if m.get("validator_ok") is not True:
        reasons.append("manifest_validator_not_ok")
    hpx_by, ray_by = island.get("hpx_by", {}), island.get("ray_by", {})
    if set(hpx_by) & set(ray_by) < set(ladder) or set(hpx_by) < set(ladder) or set(ray_by) < set(ladder):
        reasons.append("ladder_not_covered")
    for s in ladder:
        for by in (hpx_by, ray_by):
            art = by.get(s)
            if not art:
                reasons.append(f"missing_size_{s}")
                continue
            meas = art.get("measured")
            if meas is None or meas < required_measured or len(art.get("calls", [])) != meas:
                reasons.append(f"measured_lt_required_S{s}")
            try:
                if art.get("expected_digest") != payload_digest(int(art.get("x")), int(art.get("n")), s):
                    reasons.append(f"digest_oracle_mismatch_S{s}")
            except Exception:  # noqa: BLE001
                reasons.append(f"digest_oracle_uncheckable_S{s}")
    return ("clean" if not reasons else "flagged"), sorted(set(reasons))


def _band_arm_provenance(islands, *, arm):
    """Per-island provenance summary for one arm (keyed block; recorded, not equalized/differenced)."""
    by = f"{arm}_by"
    out = []
    hpx_keys = ("node_set", "hpx_poll", "hpx_runtime", "hpx_serialization", "numa_nic",
                "connector_anomaly_witness", "root_ip", "hpx_teardown_clean")
    ray_keys = ("node_set", "ray_version", "resource_map", "driver_node_id", "remote_node_ids",
                "ray_head_num_cpus", "ray_coordinator_num_cpus", "transport_family",
                "object_return_path", "plasma_engagement", "cpu_bind_mode", "no_orphan_proof")
    keys = hpx_keys if arm == "hpx" else ray_keys
    for isl in islands:
        a0 = next(iter(isl.get(by, {}).values()), {})
        rec = {"island_index": isl.get("island_index"), "job": isl.get("job")}
        for k in keys:
            rec[k] = a0.get(k, "not_observed")
        out.append(rec)
    return out


def _band_gates(islands, qualities, *, band_id, required_islands, required_measured, ladder,
                island_independence, arms):
    ladder_set = set(ladder)

    def _struct_key(isl):
        h = next(iter(isl.get("hpx_by", {}).values()), {})
        r = next(iter(isl.get("ray_by", {}).values()), {})
        return (tuple(ladder), h.get("n"), r.get("n"), h.get("measured"), r.get("measured"),
                h.get("boundary"), r.get("boundary"), h.get("clock"), r.get("clock"),
                h.get("selected_subnet"), r.get("selected_subnet"))

    struct_keys = [_struct_key(isl) for isl in islands]
    serialization_observed = bool(islands) and all(
        (next(iter(isl.get("hpx_by", {}).values()), {}).get("hpx_serialization", {})
         .get("zero_copy_runtime_path_taken", "not_observed") != "not_observed")
        for isl in islands)
    forbidden = _scan_forbidden_keys({"islands": [isl.get("manifest") for isl in islands],
                                      "arms": arms}, [])
    cross_arm = _scan_cross_arm_tokens(arms, [])
    return {
        "islands_present_ge_required": len(islands) >= required_islands,
        "band_id_present": bool(band_id),
        "all_islands_manifest_pass": bool(islands)
        and all(isl["manifest"].get("overall_manifest_pass") is True for isl in islands),
        "all_islands_same_axis": bool(islands)
        and all(isl["manifest"].get("same_axis_comparison") is True for isl in islands),
        "all_islands_validator_ok": bool(islands)
        and all(isl["manifest"].get("validator_ok") is True for isl in islands),
        "all_islands_clean_quality": bool(qualities) and all(q == "clean" for q in qualities),
        "all_islands_full_ladder": bool(islands) and all(
            (set(isl.get("hpx_by", {})) >= ladder_set and set(isl.get("ray_by", {})) >= ladder_set)
            for isl in islands),
        "all_islands_measured_ge_required": bool(islands) and all(
            all(by.get(s) and by[s].get("measured") is not None
                and by[s].get("measured") >= required_measured
                and len(by[s].get("calls", [])) == by[s].get("measured")
                for s in ladder for by in (isl.get("hpx_by", {}), isl.get("ray_by", {})))
            for isl in islands),
        "structural_params_consistent_across_islands": len(set(struct_keys)) <= 1 and bool(struct_keys),
        "island_independence_declared":
            island_independence in ("fresh_allocation_per_island", "same_allocation_rounds"),
        "no_forbidden_keys": len(forbidden) == 0,
        "no_cross_arm_arithmetic": len(cross_arm) == 0,
    }


def build_payload_band_aggregate(islands, *, band_id, required_islands=DEFAULT_REQUIRED_ISLANDS,
                                 required_measured=DEFAULT_REQUIRED_MEASURED,
                                 ladder_sizes=DEFAULT_SIZE_LADDER,
                                 island_independence="fresh_allocation_per_island"):
    """PURE band builder over R island dicts {island_index, job, manifest, hpx_by, ray_by}. Computes
    within-arm distributions per arm/size/island + across-island median/range per arm/size, in SEPARATE
    keyed arm blocks. No cross-arm arithmetic. On any gate failure the band fails closed."""
    ladder = [int(s) for s in ladder_sizes]
    islands = sorted(islands, key=lambda i: (i.get("island_index") is None, i.get("island_index")))

    quality_pairs = [_island_quality(isl, ladder=ladder, required_measured=required_measured)
                     for isl in islands]
    qualities = [q for q, _ in quality_pairs]
    island_summaries = [{"island_index": isl.get("island_index"), "job": isl.get("job"),
                         "node_set": next(iter(isl.get("hpx_by", {}).values()), {}).get("node_set"),
                         "manifest_overall_pass": isl["manifest"].get("overall_manifest_pass"),
                         "manifest_same_axis": isl["manifest"].get("same_axis_comparison"),
                         "manifest_validator_ok": isl["manifest"].get("validator_ok"),
                         "island_quality": q, "quality_reasons": reasons}
                        for isl, (q, reasons) in zip(islands, quality_pairs)]

    def _arm_block(arm):
        by = f"{arm}_by"
        by_size = {}
        for s in ladder:
            per_island = []
            for isl in islands:
                art = isl.get(by, {}).get(s, {})
                rtts = [c.get("rtt_ms") for c in art.get("calls", [])]
                stats = _within_arm_stats(rtts)
                per_island.append({"island_index": isl.get("island_index"), "job": isl.get("job"),
                                   **stats})
            p50s = [pi["p50_ms"] for pi in per_island if pi["p50_ms"] is not None]
            p90s = [pi["p90_ms"] for pi in per_island if pi["p90_ms"] is not None]
            by_size[str(s)] = {
                "payload_bytes": s,
                "per_island": per_island,
                "across_island_p50": _across_island(p50s),
                "across_island_p90": _across_island(p90s),
                "any_high_variability": any(pi["high_variability_flag"] for pi in per_island),
                "any_multimodal_suspected": any(pi["multimodal_suspected"] for pi in per_island),
            }
        return {"provenance": _band_arm_provenance(islands, arm=arm), "by_size": by_size}

    arms = {"hpx": _arm_block("hpx"), "ray": _arm_block("ray")}

    gates = _band_gates(islands, qualities, band_id=band_id, required_islands=required_islands,
                        required_measured=required_measured, ladder=ladder,
                        island_independence=island_independence, arms=arms)
    gates_all_pass = bool(gates) and all(gates.values())

    fresh = island_independence == "fresh_allocation_per_island"
    distributional_evidence = bool(gates_all_pass and fresh)   # WITHIN-ARM only
    # distributional_payload_ladder stays blocked: the HPX serialization runtime path is not observed.
    serialization_observed = gates.get("no_forbidden_keys") and all(
        (next(iter(isl.get("hpx_by", {}).values()), {}).get("hpx_serialization", {})
         .get("zero_copy_runtime_path_taken", "not_observed") != "not_observed")
        for isl in islands) if islands else False

    band = {
        "experiment": "exp64",
        "kind": "payload_band_aggregate",
        "phase": "payload-band-aggregate",
        "slice": 4,
        "title": "exp64 Slice 4 matched payload ladder band aggregate (R islands)",
        "band_id": band_id,
        "required_islands": required_islands,
        "required_measured": required_measured,
        "island_independence": island_independence,
        "ladder_sizes": ladder,
        "n_islands": len(islands),
        "islands": island_summaries,
        "evidence_grade": EVIDENCE_GRADE_BAND_R5 if gates_all_pass else "band_failed_closed",
        "distributional_evidence": distributional_evidence,
        "percentiles_evidence_ready": distributional_evidence,  # p50/p90 only
        "p99_evidence_ready": False,                            # not at measured=30
        "distributional_payload_ladder_ready": bool(gates_all_pass and serialization_observed),
        "distributional_payload_ladder_blocked_reason": list(DISTRIBUTIONAL_LADDER_BLOCKED_REASONS),
        "honesty_notes": dict(BAND_HONESTY_NOTES),
        "arms": arms,
        "band_gates": gates,
        "band_gates_all_pass": gates_all_pass,
        "same_axis_comparison": bool(gates_all_pass),   # structural-correlation flag ONLY
        "no_cross_arm_timing_computed": True,
        "arms_differenced": False,
        "ratio_reported": False,
        "speedup_computed": False,
        "placement_bands_differenced": False,
        "islands_cherry_picked": False,
        "overall_band_pass": bool(gates_all_pass),
        "created_monotonic_ns": time.monotonic_ns(),
    }
    return band


def validate_payload_band_aggregate(band):
    """Fail-closed structural verifier for a payload-band aggregate. Returns (ok, problems)."""
    problems = []
    for k in BAND_FENCE_KEYS_FALSE:
        if band.get(k) is not False:
            problems.append(f"band fence {k} must be False, got {band.get(k)!r}")
    if band.get("no_cross_arm_timing_computed") is not True:
        problems.append("no_cross_arm_timing_computed must be True")
    if band.get("distributional_payload_ladder_ready") is not False:
        problems.append("distributional_payload_ladder_ready must be False (serialization path not observed)")
    found = _scan_forbidden_keys(band, [])
    if found:
        problems.append(f"forbidden claim keys present: {found}")
    cross = _scan_cross_arm_tokens(band.get("arms", {}), [])
    if cross:
        problems.append(f"cross-arm arithmetic keys present: {cross}")
    gates = band.get("band_gates", {})
    gates_all = bool(gates) and all(gates.values())
    if band.get("band_gates_all_pass") != gates_all:
        problems.append("band_gates_all_pass inconsistent with band_gates")
    if band.get("same_axis_comparison") and not gates_all:
        problems.append("same_axis_comparison True while a band gate fails")
    if band.get("overall_band_pass") and not gates_all:
        problems.append("overall_band_pass True while a band gate fails")
    if gates_all:
        if band.get("evidence_grade") != EVIDENCE_GRADE_BAND_R5:
            problems.append("evidence_grade must be matched_band_r5 when all gates pass")
    else:
        for k in ("same_axis_comparison", "distributional_evidence", "percentiles_evidence_ready",
                  "overall_band_pass"):
            if band.get(k) is not False:
                problems.append(f"{k} must be False on any band gate failure")
        if band.get("evidence_grade") == EVIDENCE_GRADE_BAND_R5:
            problems.append("evidence_grade must not be matched_band_r5 on a failed band")
    return (len(problems) == 0, problems)


def run_payload_band_aggregate(*, band_id, required_islands=DEFAULT_REQUIRED_ISLANDS,
                               required_measured=DEFAULT_REQUIRED_MEASURED,
                               ladder_sizes=DEFAULT_SIZE_LADDER,
                               island_independence="fresh_allocation_per_island",
                               runs_dir=EXP64_RUNS, write=True):
    """Glob per-island manifests for `band_id` + their per-size arm JSONs, build + validate the band
    aggregate, write it. Skips cleanly (rc=0) when no island manifests carry the band_id."""
    import glob
    if not band_id:
        print("exp64 payload-band-aggregate: SKIP -- no --band-id provided.")
        return 0
    islands = []
    for mp in sorted(glob.glob(os.path.join(runs_dir, "exp64_payload_ladder_*_manifest.json"))):
        m = _read_json(mp)
        if not m or m.get("band_id") != band_id:
            continue
        job = m.get("job")
        hpx_by = _index_by_size([a for a in (_read_json(p) for p in sorted(glob.glob(
            os.path.join(runs_dir, f"exp64_payload_smoke_{job}_S*_hpx.json")))) if a])
        ray_by = _index_by_size([a for a in (_read_json(p) for p in sorted(glob.glob(
            os.path.join(runs_dir, f"exp64_payload_smoke_{job}_S*_ray.json")))) if a])
        islands.append({"island_index": m.get("island_index"), "job": job, "manifest": m,
                        "hpx_by": hpx_by, "ray_by": ray_by})
    if not islands:
        print(f"exp64 payload-band-aggregate: SKIP -- no island manifests for band_id={band_id} "
              f"under {runs_dir}.")
        return 0
    band = build_payload_band_aggregate(islands, band_id=band_id, required_islands=required_islands,
                                        required_measured=required_measured, ladder_sizes=ladder_sizes,
                                        island_independence=island_independence)
    ok, problems = validate_payload_band_aggregate(band)
    band["validator_ok"] = ok
    band["validator_problems"] = problems
    if write:
        path = os.path.join(runs_dir, f"exp64_payload_band_{band_id}_aggregate.json")
        with open(path, "w") as f:
            json.dump(band, f, indent=2)
    failed = [g for g, v in band["band_gates"].items() if not v]
    print(f"exp64 payload-band-aggregate band_id={band_id}: "
          f"overall_band_pass={band['overall_band_pass']} evidence_grade={band['evidence_grade']} "
          f"same_axis_comparison={band['same_axis_comparison']} "
          f"distributional_evidence={band['distributional_evidence']} "
          f"n_islands={band['n_islands']} validator_ok={ok} gates_failed={failed or 'none'}")
    if problems:
        print(f"  validator_problems={problems}")
    if write:
        print(f"  wrote exp64_payload_band_{band_id}_aggregate.json (gitignored)")
    return 0 if (band["overall_band_pass"] and ok) else 1


# ---------------------------------------------------------------------------
# Phases
# ---------------------------------------------------------------------------

def _skip(reason):
    print(f"exp64: SKIP -- {reason}")
    return 0


def phase_selftest():
    # Late import avoids a module-load cycle (selftest imports oracles from here).
    from selftest_slice0 import run_all_selftests
    return run_all_selftests()


def phase_stub(name):
    return _skip(
        f"{name}: needs the exp64 native ext / cluster (Slice 1+). Slice 0 is pure Python only.")


def main(argv=None):
    ap = argparse.ArgumentParser(description="exp64 payload-carrying fanin size sweep.")
    ap.add_argument("--phase",
                    choices=("selftest", "hpx-payload-remote-smoke", "ray-payload-remote-smoke",
                             "payload-ladder-manifest", "payload-band-aggregate",
                             "smoke", "remote-smoke", "size-sweep"),
                    default="selftest",
                    help="selftest runs the pure oracle/design layer (runs anywhere); "
                         "hpx-payload-remote-smoke runs the Slice 1 HPX-only 3-node payload smoke; "
                         "ray-payload-remote-smoke runs the Slice 2 Ray matched payload smoke (head + "
                         "2 workers; both skip cleanly off-cluster/unbuilt/without Ray); "
                         "payload-ladder-manifest pairs the Slice 3 HPX+Ray per-size artifacts for a "
                         "--job into a pure structural manifest (skips cleanly when artifacts absent); "
                         "payload-band-aggregate aggregates the Slice 4 R-island manifests for a "
                         "--band-id into a pure within-arm band (skips cleanly when absent); "
                         "smoke/remote-smoke/size-sweep remain Slice-0 skip stubs.")
    ap.add_argument("--x", type=int, default=7)
    ap.add_argument("--n", type=int, default=8, help="inner fanout N")
    ap.add_argument("--sizes", default=",".join(str(s) for s in DEFAULT_SIZE_LADDER),
                    help="comma-separated payload byte ladder (full-sweep planning default)")
    # hpx-payload-remote-smoke controls (Slice 1). Default smoke sizes: S=0 floor first, then 256KB.
    ap.add_argument("--smoke-sizes", default="0,262144",
                    help="comma-separated payload sizes for the remote smoke (S=0 first)")
    ap.add_argument("--dispatch-timeout-s", type=float, default=8.0)
    ap.add_argument("--prefer-subnet", default="10.42.5.")
    ap.add_argument("--root-port", type=int, default=7950, help="embedded-root AGAS/parcelport port")
    ap.add_argument("--await-timeout", type=int, default=60)
    ap.add_argument("--serve-timeout", type=int, default=600)
    ap.add_argument("--n-remote", type=int, default=2)
    ap.add_argument("--prewarm", type=int, default=3)
    ap.add_argument("--measured", type=int, default=5)
    # ray-payload-remote-smoke controls (Slice 2).
    ap.add_argument("--ray-port", type=int, default=6379, help="Ray GCS port for the head")
    ap.add_argument("--worker-cpus", type=int, default=8, help="num_cpus advertised by each Ray worker")
    ap.add_argument("--ray-dispatch-timeout-s", type=float, default=30.0,
                    help="bounded ray.get timeout for the coordinator (fail closed, no hang)")
    # payload-ladder-manifest controls (Slice 3).
    ap.add_argument("--job", default=None,
                    help="SLURM_JOB_ID whose per-size HPX+Ray artifacts the manifest phase pairs")
    ap.add_argument("--ladder-sizes", default=",".join(str(s) for s in DEFAULT_SIZE_LADDER),
                    help="comma-separated payload ladder the manifest expects both arms to cover")
    # Slice 4 band controls (per-island manifest tags + payload-band-aggregate).
    ap.add_argument("--band-id", default=None, help="band id tying R island manifests together")
    ap.add_argument("--island-index", type=int, default=None, help="this island's index within the band")
    ap.add_argument("--required-islands", type=int, default=DEFAULT_REQUIRED_ISLANDS,
                    help="minimum clean islands the band aggregate requires (default 5)")
    ap.add_argument("--required-measured", type=int, default=DEFAULT_REQUIRED_MEASURED,
                    help="minimum measured RTTs per size/arm the band requires (default 30)")
    ap.add_argument("--island-independence", default="fresh_allocation_per_island",
                    choices=("fresh_allocation_per_island", "same_allocation_rounds"),
                    help="fresh_allocation_per_island earns distributional_evidence; "
                         "same_allocation_rounds passes structurally but not as distributional")
    args = ap.parse_args(argv)

    if args.phase == "selftest":
        return phase_selftest()
    if args.phase == "hpx-payload-remote-smoke":
        sizes = [int(s) for s in args.smoke_sizes.split(",") if s.strip() != ""]
        return run_payload_remote_smoke(
            x=args.x, n=args.n, sizes=sizes, dispatch_timeout_s=args.dispatch_timeout_s,
            prefer_subnet=args.prefer_subnet, root_port=args.root_port,
            await_timeout=args.await_timeout, serve_timeout=args.serve_timeout,
            n_remote=args.n_remote, prewarm=args.prewarm, measured=args.measured)
    if args.phase == "ray-payload-remote-smoke":
        sizes = [int(s) for s in args.smoke_sizes.split(",") if s.strip() != ""]
        return run_ray_payload_smoke(
            x=args.x, n=args.n, sizes=sizes, prefer_subnet=args.prefer_subnet,
            n_remote=args.n_remote, ray_port=args.ray_port, worker_cpus=args.worker_cpus,
            ray_dispatch_timeout_s=args.ray_dispatch_timeout_s, prewarm=args.prewarm,
            measured=args.measured)
    if args.phase == "payload-ladder-manifest":
        ladder = [int(s) for s in args.ladder_sizes.split(",") if s.strip() != ""]
        return run_payload_ladder_manifest(job=args.job, ladder_sizes=ladder, band_id=args.band_id,
                                           island_index=args.island_index,
                                           required_measured=args.required_measured
                                           if args.band_id else None)
    if args.phase == "payload-band-aggregate":
        ladder = [int(s) for s in args.ladder_sizes.split(",") if s.strip() != ""]
        return run_payload_band_aggregate(
            band_id=args.band_id, required_islands=args.required_islands,
            required_measured=args.required_measured, ladder_sizes=ladder,
            island_independence=args.island_independence)
    return phase_stub(args.phase)


if __name__ == "__main__":
    sys.exit(main())
