# exp61 — Same-axis Python-boundary comparison: Ray actor vs experiment-only HPX/RayX

## Why exp61 exists

exp58 measured the HPX two-node path from **C++** around `hpx::async(...).get()`; exp59 measured
the Ray actor path from **Python** around `ray.get(actor.dist_probe.remote(x))`. Those are
**different caller boundaries**, so exp58/exp59 are explicitly a *plane-labeled juxtaposition*,
**not** a same-axis comparison. exp61 is the path toward a genuine same-axis number by timing
**both** paths at the **same Python caller boundary**:

```
t0 = perf_counter_ns()
result = <blocking op>          # Ray:      ray.get(actor.dist_probe.remote(x))
                                # HPX/RayX: ext.dist_probe(x)   (experiment-only)
t1 = perf_counter_ns()
```

The closed-`int64` oracle is shared with exp58: `result = (x ^ 0x52415958) + (node_tag << 1)`
(where `node_tag` is the HPX `locality_id` on the HPX arm, and a fixed explicit tag on the Ray arm).

## Slice 0 — HPX-side embedding smoke ONLY

Slice 0 retires the **single biggest risk**: can a Python process embed an HPX runtime via a
pybind extension, dispatch a fixed closed-`int64` HPX action, and get the result back to Python
with a clean lifecycle?

```
Python caller -> pybind/HPX extension (dist_probe_ext) -> hpx::async<exp61_dist_probe_action>(find_here, x).get() -> result -> Python
```

* **Single-locality (`find_here`)** — no connector, no parcelport, no distributed locality. The
  action runs on the embedded root locality (id 0), so `node_tag = 0` and `oracle = x ^ 0x52415958`.
* **Embedding pattern** mirrors `python/src/rayx/_rayx.cpp`: `hpx::start(nullptr, argc, argv,
  params)` on import-time `start()`, each call via `hpx::run_as_hpx_thread([...])` with
  `py::gil_scoped_release` around the blocking call, teardown via `hpx::post(finalize)` +
  `hpx::stop()`.
* **No Ray side yet.** The Ray `ray.get(...)` arm and the same-axis aggregate are deferred to a
  later slice.

### Files (experiment-only, under `experiments/61_python_boundary_same_axis_ray_vs_rayx/`)

| file | role |
|---|---|
| `shared_dist_probe.hpp` | the closed-`int64` `exp61_dist_probe_action` + oracle (same contract as exp58) |
| `dist_probe_ext.cpp` | the pybind11 module embedding HPX: Slice 0 `start`/`dist_probe`/`local_locality_id`/`shutdown`; Slice 2A adds `await_remote`/`dist_probe_remote`/`remote_locality_id`/`hostname` (root/remote API) |
| `dist_probe_connector.cpp` | Slice 2A: standalone connect-mode HPX remote locality serving the same action (`--probe-info` off-cluster sanity mode) |
| `CMakeLists.txt` | standalone build of `dist_probe_ext` (module) **and** `dist_probe_connector` (executable) against the installed HPX prefix + venv pybind11 |
| `run_exp61_same_axis.py` | runner: `--phase hpx-smoke`/`ray-smoke` (Slice 0/1), `--phase hpx-connected`/`ray-remote` (Slice 2A, two-node-gated), `--phase selftest` (pure-Python) |
| `.gitignore` | ignores `build/`, `_exp61_runs/`, `*.so`, logs, redirected/skip/fail siblings; keeps the four curated `*_aggregate.json` names trackable |

The single-locality Slice-0 smoke needs no connector; `dist_probe_connector.cpp` is added in Slice 2A
as two-node-capable scaffolding and is run on a second node only on a real Slurm allocation (Slice 2B).

### Build + run (local; no Rostam)

```bash
cd experiments/61_python_boundary_same_axis_ray_vs_rayx
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release -DPYBIND11_FINDPYTHON=ON \
    -DCMAKE_PREFIX_PATH="<hpx-install>;$(python -m pybind11 --cmakedir)"
cmake --build build
python run_exp61_same_axis.py --phase hpx-smoke
python run_exp61_same_axis.py --phase ray-smoke   # local Ray actor; skips cleanly if Ray absent
python run_exp61_same_axis.py --phase selftest   # pure-Python, no build needed
```

The runner **skips cleanly** (no aggregate written) if `dist_probe_ext` is not importable
(hpx-smoke) or if `ray` is not installed (ray-smoke).

### Artifact

`hpx_smoke_aggregate.json` carries the claim-fence fields directly: `experiment_id`, `phase`,
`overall`, `same_axis_comparison=false`, `comparison_kind`,
`measurement_boundary_hpx=python_caller_perf_counter_ns_around_blocking_call`,
`clock=perf_counter_ns`, `k`, `w`, `prewarm`, `qd1_only=true`,
`pipeline_excluded_from_comparison=true`, `hpx_extension_experiment_only=true`,
`hpx_gil_released`, `hpx_oracle_correct`, `same_axis_claim_allowed=false`,
`speedup_computed=false`, `ratio_reported=false`, `forbidden_claims`. The Python-boundary call
time is recorded as **observation-only** (not a band, not a claim).

## What Slice 0 does and does not establish

**Establishes (if it passes):** an embedded HPX runtime can serve a fixed closed-`int64` action to
a Python caller and return the correct value, with the GIL released around the blocking call and a
clean start/stop lifecycle — the mechanism the eventual same-axis measurement depends on.

**Does NOT establish:** any same-axis comparison, any Ray-vs-HPX number, any speedup/ratio, any
“HPX beats Ray” / “RayX makes Ray faster”, any two-node/distributed/fabric result, or any change
to the shipped `rayx.runtime` API. This is **not** the HPX-calls-Python-callback experiment, and
it is **not** public distributed RayX.

## Slice 1 — Ray-side smoke at the SAME Python boundary

Slice 1 adds the **Ray arm** (`--phase ray-smoke`): a **local single-node** Ray actor timed at the
exact same Python caller boundary as the HPX arm, through **one shared timing helper** so the two
arms can never drift into different timing semantics.

```
t0 = perf_counter_ns(); result = ray.get(actor.dist_probe.remote(x)); t1 = perf_counter_ns()
```

* **One harness for both arms.** `timed_qd1(call, x, expected, k, w)` drives any blocking callable:
  1 prewarm (absorbs spin-up) + `W` warmups (dropped) + `K` timed `perf_counter_ns` measurements,
  with a **per-call oracle check**. hpx-smoke passes it `ext.dist_probe`; ray-smoke passes it
  `lambda x: ray.get(actor.dist_probe.remote(x))`. Same clock, same `K`/`W`/prewarm, QD1,
  blocking-only, no pipelining.
* **Same closed-`int64` oracle family.** The Ray actor computes `(x ^ 0x52415958) + (tag << 1)`. For
  this single-node smoke the `node_tag` is a **fixed, explicit** value (`--ray-node-tag`, default
  `0`) recorded in the artifact. It need **not** match the HPX `locality_id` yet and is **not** a
  placement proof.
* **Skips cleanly** if Ray is not installed: prints a SKIP line, exits `0`, writes no aggregate.
* **Separate artifact.** The Ray arm writes `ray_smoke_aggregate.json`; the HPX arm writes
  `hpx_smoke_aggregate.json`. They are **never** differenced or ratioed here.

### Shared fence/harness fields (both arms)

Both aggregates carry identical boundary/harness provenance via `_fence_fields(...)`:
`experiment_id`, `phase`, `overall`, `comparison_kind`, `same_axis_comparison=false`,
`same_axis_claim_allowed=false`, `speedup_computed=false`, `ratio_reported=false`, `qd1_only=true`,
`pipeline_excluded_from_comparison=true`, `clock=perf_counter_ns`, `k`, `w`, `prewarm`,
`measurement_boundary=python_caller_perf_counter_ns_around_blocking_call`, a `shared_harness` block,
and `forbidden_claims`. The ray-smoke aggregate adds `measurement_boundary_ray`, `ray_side_measured`,
`ray_call_primitive`, `ray_actor_local_only=true`, `ray_version`, `ray_node_tag`, `ray_oracle_correct`,
and `ray_p50_ns`/`ray_p90_ns`/`ray_p99_ns`/`ray_mean_ns` as **observation-only** (not a band, not a
claim).

### What Slice 1 does and does not establish

**Establishes (if it passes):** the Ray and HPX arms can both be driven by **one identical Python
timing harness** at the **same caller boundary**, each returning the correct closed-`int64` value
under per-call oracle validation, with clean lifecycle — the precondition for an eventual same-axis
measurement.

**Does NOT establish:** any same-axis comparison, any Ray-vs-HPX number, any speedup/ratio, any
“HPX beats Ray” / “Ray is slower than HPX” / “RayX makes Ray faster”, any two-node/placement result,
or any change to the shipped `rayx.runtime` API. The two arms remain **separate artifacts**; the
connector and two-node placement gates are **deferred**.

## Slice 2A — two-node-capable scaffolding (NOT yet run on two nodes)

Slice 2A adds the **code** for a two-node path on both arms and wires strict gates so it **skips
cleanly off-cluster** and can never fabricate a two-node result locally. **Nothing here is a
two-node measurement** — that is Slice 2B on Rostam.

**Clearly separate the two states:**

| | implemented / scaffolded (Slice 2A) | proven on two nodes (Slice 2B, pending) |
|---|---|---|
| HPX connector binary | ✅ builds + links; `--probe-info` works | ⛔ remote join/serve over TCP unproven |
| HPX root remote API | ✅ `await_remote`/`dist_probe_remote` compiled | ⛔ remote dispatch RTT unmeasured |
| Ray remote placement | ✅ hard-pin + off-node verification coded | ⛔ off-node actor unrun |
| local run result | **skip-clean, exit 0, no curated aggregate** | curated `*_aggregate.json` only on a real pass |

### HPX arm — `--phase hpx-connected`

* **`dist_probe_connector.cpp`** (new, experiment-only): a standalone **connect-mode** HPX locality.
  It registers the **same** `exp61_dist_probe_action` (via `shared_dist_probe.hpp`, a *separate*
  binary so `HPX_PLAIN_ACTION` registers once per binary), joins the root's AGAS, attests
  hostname/locality-id/advertised-IP, waits for the root's served marker, and disconnects cleanly
  (`post(disconnect)+stop`) — copy-in-spirit of the exp58 connector role. No timing of its own; the
  single clock stays on the Python caller. A `--probe-info` mode prints hostname/endpoint **without**
  starting HPX, for off-cluster sanity checks.
* **`dist_probe_ext.cpp`** gains an *additive* root/remote API: `start(hpx_threads, extra_args)` (the
  `extra_args` carry two-node networking flags so the embedded runtime is the **AGAS root**),
  `await_remote(timeout_s)` (polls `find_all_localities()`, caches the joined remote id **once**, or
  returns `-1`), `dist_probe_remote(x)` (dispatch to the **cached remote** locality, never
  `find_here`), `remote_locality_id()`, `hostname()`. **The Slice-0 single-locality smoke is
  byte-for-byte unchanged** (no extra args ⇒ same console runtime; `await_remote` finds no remote ⇒
  `dist_probe_remote` refuses — there is no single-node fallback).
* The runner builds the HPX argv for the root, launches the connector on the remote node via `srun`,
  awaits the remote locality, then times `ext.dist_probe_remote(x)` through the **same `timed_qd1`
  harness**. `node_tag` is the **remote locality id**; oracle `= (x ^ 0x52415958) + (node_tag << 1)`.

> The two-node orchestration body (`_run_hpx_connected_two_node`) is **unvalidated Slice-2A
> scaffolding** — the exact HPX networking flags (`--hpx:hpx`/`--hpx:agas`/
> `--hpx:expect-connecting-localities`), the root IP/subnet selection, and the `srun` pinning are
> **confirmed/tuned in Slice 2B**. It runs only inside a real ≥2-node Slurm allocation and any
> failure is captured as `overall="fail"` (written to an ignored sibling), never a fake pass.

### Ray arm — `--phase ray-remote`

* Connects to an **existing** ≥2-node Ray cluster (`--ray-address` / `RAY_ADDRESS` / `auto`) — it
  **never starts a local cluster**. Requires ≥2 distinct alive node ids, hard-pins the actor with
  `NodeAffinitySchedulingStrategy(node_id=<off-driver node>, soft=False)`, and **verifies the actor
  is genuinely off the driver node** (`actor.where()`) before timing anything. Then times
  `ray.get(actor.dist_probe.remote(x))` through the **same `timed_qd1` harness**. **exp59 numbers are
  not reused.**

### Strict skip behavior (the core of Slice 2A)

Both phases skip cleanly (print a SKIP line, **exit 0**, write **no curated aggregate**) when: not in
a ≥2-node Slurm allocation; Ray missing / no live ≥2-node cluster; or the connector/extension is not
built. On skip/fail they write **only** an *ignored* `<base>_skip.json` / `<base>_fail.json` sibling
carrying full provenance with `two_node_exercised=false` — the curated `hpx_connected_aggregate.json`
/ `ray_remote_aggregate.json` names stay **empty until a real Slice-2B pass**.

### Placement / provenance fields (present even when null on a local skip)

`hpx-connected`: `hpx_connector_used=true`, `hpx_root_hostname`, `hpx_connector_hostname`,
`hpx_root_ip`, `hpx_connector_ip`, `hpx_root_locality_id`, `hpx_remote_locality_id`,
`hpx_here_locality_differs_from_remote`, `hpx_parcelport="tcp"`, `hpx_tcp_nodelay_verified`,
`hpx_action_registration_name`, `hpx_oracle_correct`, plus the shared fences.
`ray-remote`: `ray_placement_strategy="NodeAffinitySchedulingStrategy(soft=False)"`, `ray_node_ids`,
`ray_hostnames`, `ray_ips`, `ray_driver_node_id`, `ray_target_node_id`, `ray_actor_remote_only=true`,
`ray_off_node_sample_count`, `ray_oracle_correct`, plus the shared fences. All four arms still share
the identical `measurement_boundary` string and keep `same_axis_comparison=false`,
`same_axis_claim_allowed=false`, `speedup_computed=false`, `ratio_reported=false`.

### What Slice 2A does and does not establish

**Establishes:** the two-node-capable code exists, compiles/links (both `dist_probe_ext` and
`dist_probe_connector`), drives both arms through the one shared harness, and **gates itself so a
local run is always a clean skip** — no local fallback is ever labeled remote.

**Does NOT establish:** any two-node result, any remote-dispatch or off-node-actor timing, any
placement proof, any same-axis comparison, any speedup/ratio, or any “HPX beats Ray” / “Ray is slower
than HPX” / “RayX makes Ray faster”. It adds **no** distributed action to the shipped `rayx.runtime`
API. Being a reachable remote locality is a **mechanism**, not a performance result.

## Slice 2B — first Rostam two-node run (medusa00/medusa01, `10.42.5.x`)

A manual two-node run on Rostam produced:

* **`ray-remote`: PASS.** The actor was hard-pinned **off the driver node** with
  `NodeAffinitySchedulingStrategy(soft=False)` (driver on medusa00, actor on medusa01),
  `ray_off_node_sample_count=5`, oracle correct on every call, with full placement provenance (two
  Ray node ids, hostnames, `10.42.5.30/.31`). This is real evidence of the **off-node placement
  mechanism** — nothing more (no timing comparison, no ratio).
* **`hpx-connected`: FAIL**, before any remote join, exposing **two orchestration bugs**:
  * **Bug A — step-scoped Slurm detection.** When the driver runs as a one-node `srun` step of a
    two-node job, it saw `SLURM_JOB_NODELIST=medusa00` / `SLURM_NNODES=1` and false-skipped, even
    though the allocation was `medusa[00-01]`.
  * **Bug B — embedded HPX root vs Slurm batch env.** `hpx::start` failed with *"Requested AGAS host
    (10.42.5.30) not found in node list"* because HPX built its node list from the Slurm **hostname**
    batch env, which doesn't contain our explicit **IP** endpoint. (A latent third bug: the connector
    launch passed `--hpx:hpx={root_ip}:0`, i.e. the *root's* IP as the *connector's* bind address on
    medusa01.)

### This patch (local; HPX arm only — the Ray pass is unchanged)

* **Bug A fix** — `slurm_two_node_from_env` now detects the **allocation**, not the current step:
  it consults the authoritative `scontrol show job "$SLURM_JOB_ID"` (`NumNodes`/`NodeList`) and the
  `SLURM_NODELIST` alternate, taking the **largest** node count across all signals, so a shrunk
  one-node step can't mask a two-node job. Off-cluster (no `SLURM_JOB_ID`) still skips cleanly; no
  over-counting.
* **Bug B fix** — for `hpx-connected` only: the embedded root now starts with
  **`--hpx:ignore-batch-env`** ahead of the explicit IP endpoints, so HPX uses our `10.42.5.x`
  AGAS/parcelport endpoints instead of the Slurm hostname node list. The **connector self-binds its
  own node's selected-subnet IP** via a new `--prefer-subnet` option (getifaddrs lookup of the first
  `10.42.5.x` address on medusa01) instead of the root IP, and also ignores the batch env. The
  preferred subnet, ports, root extra-args, and the connector's self-bound IP are recorded in the
  artifact (`hpx_prefer_subnet`, `hpx_root_port`, `hpx_ignore_batch_env`, `hpx_root_extra_args`,
  `hpx_connector_self_binds_own_subnet_ip`, `slurm_alloc_source`, connector `attest_connect.json`).

**Validated locally:** `py_compile`, `selftest` (32/32, incl. step-scoped + `scontrol`-job detection
and off-cluster safety), `hpx-connected` / `ray-remote` skip cleanly off-cluster, and both binaries
(`dist_probe_ext`, the patched `dist_probe_connector`) compile/link. **Actual HPX two-node remote
proof still requires a manual Rostam rerun** — the embedded-root-under-Slurm flags can only be
confirmed on the allocation.

### Slice 2B ACCEPTED — both arms proven on Rostam

After the Bug A/B patch, the HPX rerun **passed** on medusa00→medusa01: root locality 0 on
medusa00 (`10.42.5.30`), connector locality 1 on medusa01 (`10.42.5.31`) which **self-bound its own
subnet IP**, `hpx_here_locality_differs_from_remote=true`, oracle correct on all calls, with
`hpx_ignore_batch_env=true` and `slurm_alloc_source=scontrol_job` confirming both fixes. Together
with the earlier Ray pass, Slice 2B now has **two independent two-node mechanism proofs**
(`hpx_connected_aggregate.json`, `ray_remote_aggregate.json`). They are **independent runs** —
different jobs, different K/W — so they are **mechanism proofs only, NOT a same-axis comparison**.

## Slice 3 — matched-run structure (one allocation, both arms)

Slice 3 adds the smallest structure to run **both arms in one allocation** and prove they shared the
same conditions — **still with no timing conclusion**.

* **`--matched-run-id <tag>`** on `hpx-connected` and `ray-remote` routes each arm to a **pair-scoped**
  artifact `slice3_<tag>_hpx.json` / `slice3_<tag>_ray.json` (the Slice-2B curated
  `*_aggregate.json` files are **never** touched).
* Both arms now record **normalized shared provenance**: `matched_run_id`, `selected_subnet`,
  `node_pair` (ordered short hostnames, driver/root → actor/remote), `slurm_job_id`, plus the
  already-shared `k`/`w`/`prewarm`/`clock`/`measurement_boundary`.
* **Ray arm**: records `selected_subnet` from `--prefer-subnet` and adds a gate
  `driver_target_ips_on_selected_subnet` (driver+target IPs must be on it); keeps the hard
  `NodeAffinitySchedulingStrategy(soft=False)` and the no-local-fallback rule.
* **HPX arm**: records `selected_subnet` consistently; keeps the root/connector locality + self-bind
  provenance and the Bug A/B fixes.
* **`--phase pair-manifest --matched-run-id <tag>`** reads the two pair-scoped artifacts and writes a
  curated `slice3_<tag>_manifest.json`, setting `matched_structure_validated=true` **only if** both
  `overall=pass`, both `two_node_exercised=true`, both oracles true, and the two arms agree on
  `matched_run_id`, `slurm_job_id`, `node_pair`, `selected_subnet`, `k`, `w`, `prewarm`, `clock`, and
  the Python caller boundary. If either pair-scoped artifact is missing it **skips cleanly and writes
  nothing** — no success is ever claimed without both passing artifacts.

The manifest keeps **`same_axis_comparison=false`**: it carries each arm's boundary times for
provenance only and **never differences or ratios them**. Artifact hygiene: `slice3_*_manifest.json`
is trackable; the raw `slice3_*_{hpx,ray}.json` siblings are gitignored.

### What Slice 3 does and does not establish

**Establishes:** a *matched-run structure* — both arms ran in one allocation / node pair / subnet /
K-W-prewarm / Python boundary, each passing its own oracle, with a single manifest proving it.

**Does NOT establish:** any same-axis comparison, speedup, ratio, or HPX-vs-Ray conclusion at the
Slice 3 level itself; a single matched smoke `same_axis_comparison` stays `false`. The R=5 band that
*can* flip the same-axis gate is Slice 4, below.

## Slice 4 — R=5 matched same-axis band (completed)

The Slice 4 band aggregator is **implemented, selftested, and run on Rostam** (job `158724`, R=5; the
result is recorded below). It consumes `R>=5` matched islands — each a `slice3_<id>_manifest.json`
plus its two pair-scoped `slice3_<id>_{hpx,ray}.json` arm artifacts — and summarizes **each arm
separately** across islands.

* **`--phase band-aggregate --matched-run-ids id1,…,idR --band-out slice4_band_<job>_aggregate.json`**
  reads every manifest and both arm artifacts per island, gates them, and writes a trackable band
  aggregate. `comparison_kind` is `r5_matched_same_axis_band_no_ratio`.
* For each arm it records per-island `p50/p90/p99/mean` and an across-island median / min / max /
  spread. The two arms are **two fully separate sub-trees**: nothing relates one arm's numbers to the
  other.
* **`same_axis_comparison` becomes `true` only if every Slice 4 gate passes:** `R>=5`, every island's
  manifest + both arm artifacts present and `overall=pass`, every manifest
  `matched_structure_validated=true` with all per-island checks true, both arms `two_node_exercised`,
  both oracles true, cross-island agreement on `slurm_job_id` / `node_pair` / `selected_subnet` / `k`
  / `w` / `prewarm` / `clock` / Python caller boundary, and a captured `perf_counter_ns` clock
  overhead. Any single failing gate keeps `same_axis_comparison=false`.
* **`speedup_computed`, `ratio_reported`, and `arms_differenced` are hard-locked `false`** regardless
  of gate outcome — the band reports the two arms side by side and never subtracts or ratios them.
* `--phase manifest-summary` (Slice 3R) reuses the same band loader for an orchestration-stability
  summary over repeated matched smokes; it keeps `same_axis_comparison=false`.

Artifact hygiene: `slice4_band_*_aggregate.json` and `slice3r_*_summary.json` are trackable; the
pair-scoped raw `slice3_*_{hpx,ray}.json` siblings stay gitignored.

Even on a fully passing R=5 band, the only licensed statement is the narrow one: *for this closed-int64
QD1 micro-call on medusa00 → medusa01, measured at the same Python caller boundary, Ray actor RTT was
X and the experiment-only Python→HPX action RTT was Y.* No general "HPX beats Ray", no "RayX makes Ray
faster", no production / inference / fault-tolerance / multi-node-scaling / object-store claim, and no
implication that public `rayx.runtime` gained distributed actions.

### Slice 4 result (job 158724)

Run on Rostam in one two-node allocation, **medusa00 → medusa01**, subnet `10.42.5.`, R=5 matched
islands `band_158724_i{1..5}`, each running both arms at **K=1000 / W=100 / prewarm=1**,
`clock=perf_counter_ns`, measurement boundary
`python_caller_perf_counter_ns_around_blocking_call`, QD1 closed-`int64` workload.

**Gates — all passed (`overall=pass`, no failed gates):** R=5; every island's manifest + both arm
artifacts present and `overall=pass`; every manifest `matched_structure_validated=true` with all
per-island checks true; both arms `two_node_exercised=true`; both oracles correct on all calls;
cross-island agreement on `slurm_job_id` / `node_pair` / `selected_subnet` / `k` / `w` / `prewarm` /
`clock` / measurement boundary; `perf_counter_ns` clock overhead captured (median **92 ns**, count
4096). Resulting flags: `same_axis_comparison=true`, `comparison_kind=r5_matched_same_axis_band_no_ratio`,
and `speedup_computed=false`, `ratio_reported=false`, `arms_differenced=false`.

**Artifacts:**

* `slice4_band_158724_aggregate.json` (trackable — the band aggregate).
* `slice3_band_158724_i{1..5}_manifest.json` (trackable — the five per-island matched manifests).
* `slice3_band_158724_i{1..5}_{hpx,ray}.json` (gitignored, kept locally — the raw per-arm artifacts).

**Per-arm RTT bands (across-island median of each island's percentiles; the two arms are summarized
separately and never differenced):**

| arm | call primitive | p50 | p90 | p99 | mean |
|---|---|---|---|---|---|
| Ray actor path | `ray.get(actor.dist_probe.remote(x))` | ~518.3 µs | ~850.7 µs | ~1125.7 µs | ~584.7 µs |
| experiment-only Python→HPX action path | `ext.dist_probe_remote(x)` | ~184.8 µs | ~257.5 µs | ~322.6 µs | ~188.7 µs |

**What this licenses:** *for this QD1 closed-`int64` micro-call on medusa00 → medusa01, measured at
the same Python caller boundary in matched R=5 runs, the Ray actor path and the experiment-only
Python→HPX action path produced the per-arm RTT bands above.* Both arms are now on the **same
measurement axis** (the Python caller boundary), which is what exp58/exp59 lacked.

**What is NOT claimed:** no general "HPX beats Ray", no "RayX makes Ray faster", no production
distributed RayX, no real-inference performance, no Ray Serve / object-store / task-semantics
comparison, no fault-tolerance or scaling result, and no speedup or ratio. The arms are reported side
by side only; the band never subtracts or ratios them. This experiment-only Python→HPX binding does
**not** imply the public `rayx.runtime` API gained distributed actions.

## Slice 5 — same-node matched control band (COMPLETED; job 158734, medusa00)

Slice 4 measured the cross-node band (medusa00 → medusa01). Slice 5 adds the **within-arm placement
control**: the **same** QD1 closed-`int64` workload, at the **same** Python caller boundary, with the
**same** R=5 matched-band structure and the **same** no-ratio / no-speedup / no-cross-arm-differencing
discipline — but with **caller and callee/actor on ONE node**. It isolates a single variable, physical
co-location, for **each arm separately**. It is **not** a new experiment; it is an exp61 control that
sits beside the Slice-4 cross-node band.

**Each arm keeps its exact mechanism; only placement changes:**

* **HPX same-node (`hpx-colocated`)** — still **two DISTINCT HPX localities** (root id 0 + connector
  id 1) over the **TCP parcelport on ONE host**, dispatched via `ext.dist_probe_remote(x)`. This is
  **loopback** TCP: the parcelport software path (serialization, AGAS, parcel handling, thread
  handoff) is held constant and only the physical NIC/switch hop is removed. It is **NOT** `find_here`
  and **NOT** an in-process/local shortcut — that would change the mechanism, not just the placement.
* **Ray same-node (`ray-colocated`)** — still a **genuine remote actor** (`ray.get(actor.dist_probe
  .remote(x))`) hard-pinned to the **driver's OWN node** with `NodeAffinitySchedulingStrategy(
  node_id=driver_node, soft=False)` and verified on-node before timing. It is not an in-process call.

### What it will test

A same-node R=5 band proving each arm ran both same-node sub-runs in one allocation on one node, each
passing its own closed-`int64` oracle, with the two arms summarized **separately** and **never**
differenced or ratioed — and the same-node band **never** differenced against the Slice-4 cross-node
band. Same-node vs cross-node is described only as a **within-arm placement control**, not a broad
performance claim.

**Expectation (set up front):** for a QD1 closed-`int64` micro-call the dominant cost is parcel
serialization + HPX thread handoff + scheduler wakeup + the external-thread `run_as_hpx_thread` hop,
**not** the wire — so a clean same-node band should be flat and low, not dominated by the loopback hop.
The corrected run (below, job 158734) bears this out for both arms. A first attempt (job 158732) instead
produced a high, quantized HPX same-node tail; an audit traced that to a **connector-binding /
resource-shape confound** (a single-thread connector), not the loopback path — see the audit note in the
result section. The numbers below are observation-only and machine-specific.

### HPX-correctness gates (the same-node band cannot pass without them)

* **Disjoint core binding (enforced + verified, not just recorded)** — the two same-node localities
  must be pinned to **non-overlapping** cores, and the gate is `disjoint_core_binding_verified`, which
  passes only from **effective** (not requested) affinity on **both** sides:
  * the **root** process enforces `--root-cpuset` via `os.sched_setaffinity` **before** embedding the
    HPX runtime (with `--hpx:bind=none` so HPX does not re-pin), then reads back
    `os.sched_getaffinity` → `hpx_root_cpuset_effective` / `hpx_root_affinity_enforced`;
  * the **connector** attests its own `sched_getaffinity` (after the srun `--cpu-bind` launch) into
    `attest_connect.json` → `hpx_connector_cpuset_effective` / `hpx_connector_affinity_enforced`;
  * `disjoint_core_binding_verified` is true **only if both sides are enforced and their effective
    cpu sets are non-empty and disjoint**. Requested-only metadata (`disjoint_core_binding_recorded`)
    is kept for provenance but **cannot** pass the gate. Non-Linux / mismatch / overlap fail closed.
* **TCP_NODELAY verified (real socket-level attestation)** — `hpx_tcp_nodelay_verified` must be true
  (a QD1 micro-RTT is Nagle-sensitive on loopback and NIC alike). This is now backed by a **real
  getsockopt attestation**, not a config assumption and not a faked value:
  * After parcels have flowed (the root dispatches the action during timing, warming the parcelport
    connection), the connector enumerates **its own process's** live TCP socket fds (`/proc/self/fd`,
    Linux), keeps those whose **peer IP is the HPX root parcelport/AGAS host**, and runs
    `getsockopt(IPPROTO_TCP, TCP_NODELAY)` on each. These are the **actual** sockets the embedded HPX
    connector holds to the root locality — **not** an unrelated dummy socket we opened.
  * It writes the result to `attest_connect.json`: `tcp_nodelay_attested` (a real socket was sampled),
    `tcp_nodelay` (every matched socket had NODELAY enabled), `tcp_nodelay_source="getsockopt"`,
    `tcp_nodelay_scope="hpx_root_parcelport_peer_sockets"`, `tcp_nodelay_matched_sockets`,
    `tcp_nodelay_peer`, and `tcp_nodelay_error`. This is **socket-level** verification (getsockopt on
    the live HPX peer sockets), not HPX-runtime-config attestation.
  * **Honest scope:** matching is by **peer IP** (the root parcelport host); HPX accepted data sockets
    carry an ephemeral peer port, so a port match is deliberately not required. In this experiment the
    only TCP connections to the root IP are HPX parcelport/AGAS sockets.
  * **FAIL-CLOSED:** non-Linux platforms, no warm socket found at attest time, or a `getsockopt` error
    all leave `tcp_nodelay_attested=false` → `hpx_tcp_nodelay_verified=false` → the same-node band
    cannot flip `same_axis_comparison=true`. No value is ever fabricated.

### Phases, gates, and artifact fences

* **Phases:** `hpx-colocated`, `ray-colocated`, `pair-manifest-samenode`, `band-aggregate-samenode`.
* **HPX gates:** `slurm_present`, `connector_built`, `ext_remote_capable`, `hpx_started`,
  `second_locality_joined`, `localities_distinct`, `same_node_colocated`, `tcp_parcelport`,
  `hpx_tcp_nodelay_verified` (fail-closed), `disjoint_core_binding_verified` (fail-closed; effective
  affinity, not requested-only), `prewarm_correct`, `every_call_oracle_correct`, `no_error`.
* **Ray gates:** `slurm_present`, `ray_imported`, `ray_cluster_connect`, `actor_on_driver_node`,
  `prewarm_correct`, `every_call_oracle_correct`, `no_error`.
* **Fences (in every artifact):** `placement_mode="same_node"`; `same_axis_comparison` may become true
  **only** if every same-node band gate passes; `speedup_computed=false`, `ratio_reported=false`,
  `arms_differenced=false`, `placement_bands_differenced=false` are hard-locked.

### Artifacts

| name | role | tracked? |
|---|---|---|
| `slice5_sn_<id>_hpx.json` / `slice5_sn_<id>_ray.json` | raw pair-scoped per-arm artifacts | gitignored |
| `slice5_sn_<id>_manifest.json` | per-island same-node matched-structure manifest | trackable |
| `slice5_samenode_band_<job>_aggregate.json` | the R≥5 same-node control band aggregate | trackable |

The Slice-4 cross-node curated files are **never** touched (distinct namespace).

### Slice 5 result (job 158734 — corrected resource shape)

Run on Rostam in one exclusive single-node allocation, **medusa00**, subnet `10.42.5.`, **R=5** matched
islands `sn_band2_158734_i{1..5}`, both arms at **K=1000 / W=100 / prewarm=1**, `clock=perf_counter_ns`,
measurement boundary `python_caller_perf_counter_ns_around_blocking_call`, QD1 closed-`int64` workload.
HPX root: **4 threads** pinned to cpuset `0–3` (`os.sched_setaffinity` + `--hpx:bind=none`); connector:
**4 threads** pinned to cpuset `4–7` (`srun --cpu-bind=mask_cpu` + explicit `--hpx:threads=4` +
`--hpx:bind=none`); per-island HPX ports. The Ray arms ran in a separate phase with **no Ray head
co-resident** during the HPX arms.

**Gates — all passed (`overall=pass`, no failed gates):** R=5; every island's manifest + both arm
artifacts present and `overall=pass`; every manifest `matched_structure_validated=true`; both arms
`same_node_exercised`; both oracles correct; cross-island agreement on `slurm_job_id` / `node_single` /
`selected_subnet` / `k` / `w` / `prewarm` / `clock` / boundary / `placement_mode`; `perf_counter_ns`
clock overhead captured (median 83 ns). Same-node witnesses, **all five islands**:
`all_hpx_nodelay_verified=true` (getsockopt on **8** live HPX parcelport peer sockets per island),
`all_hpx_disjoint_core_binding` verified true (root effective `[0,1,2,3]`, connector effective
`[4,5,6,7]`, 4 threads each), `all_hpx_same_node_colocated=true` (locality ids 0/1, one host),
`all_ray_actor_on_driver_node=true` (5/5 on-node samples). Resulting flags: `same_axis_comparison=true`,
`comparison_kind=r5_matched_same_node_band_no_ratio`, `speedup_computed=false`, `ratio_reported=false`,
`arms_differenced=false`, `placement_bands_differenced=false`.

**Artifacts (canonical Slice 5 evidence):**

* `slice5_samenode_band_158734_aggregate.json` (trackable — the same-node band aggregate).
* `slice5_sn_sn_band2_158734_i{1..5}_manifest.json` (trackable — the five per-island matched manifests).
* `slice5_sn_sn_band2_158734_i{1..5}_{hpx,ray}.json` (gitignored, kept locally — the raw per-arm artifacts).

**Per-arm same-node RTT band (across-island median of each island's percentiles; the two arms are
summarized separately and NEVER differenced, ratioed, or ranked):**

| arm | call primitive | p50 | p90 | p99 | mean |
|---|---|---|---|---|---|
| Ray actor path | `ray.get(actor.dist_probe.remote(x))` | ~519.1 µs | ~790.9 µs | ~1028.6 µs | ~559.0 µs |
| experiment-only Python→HPX action path | `ext.dist_probe_remote(x)` | ~93.0 µs | ~102.3 µs | ~112.9 µs | ~94.1 µs |

**Observation-only note:** with comparable per-locality resources (4 threads / 4 cores each, disjoint),
the experiment-only HPX same-node arm is **tight and consistent** across islands (per-island p99
~108–127 µs); the quantized multi-millisecond tail seen in the superseded run (below) is **absent**. One
island recorded a single isolated max spike (~34 ms on one call out of 1000; p99 unaffected) — an OS/
scheduler hiccup, not a systematic tail. These numbers are observation-only and machine-specific. Slice
5 is a **placement control that passed its gates**, not a measurement to rank arms by.

### Audit note — superseded run 158732 (resource-shape confound)

An earlier same-node band (**job 158732**) reported a high, quantized HPX same-node tail (p50 ~1003 µs,
p90 ~4005 µs, p99 ~6975 µs). A skeptical audit found this was a **configuration / resource-shape
confound, not a loopback-placement effect and not an implementation bug**: the connector was launched
with `srun --cpu-bind=map_cpu:<cpuset>`, which binds a **single core per task**, so the connector
locality saw only one CPU and ran **one worker thread**; at QD1 that single thread repeatedly entered HPX
idle backoff, adding quantized wakeup latency. The connector launch was corrected to a CPU **mask**
(`--cpu-bind=mask_cpu`) plus an explicit `--hpx:threads` and `--hpx:bind=none`; an HPX-only probe (job
158733) confirmed the tail collapsed (p99 ~112 µs), and the corrected R=5 band (job **158734**, above)
is the canonical Slice 5 evidence. 158732 was minimum-latency-healthy (~90 µs min) and passed every
structural gate, so it remains a valid mechanism pass; only its **timing band** was confounded and is
**not** used. A separate orchestration note: 158732 also followed a cancelled attempt (158731) where a
Ray head co-resident during the HPX phase coincided with a connector crash and a root hang on
`hpx::async(remote).get()`; the successful runs ran HPX with no Ray co-resident. **Robustness
follow-up (not done here):** a bounded dispatch-side timeout in `hpx-colocated` would turn a connector
crash into a clean per-island failure instead of a hang.

### What Slice 5 establishes / does not

**Establishes:** a same-node R=5 matched placement control on the same Python caller boundary, all gates
passed, with enforced+verified disjoint affinity, real getsockopt TCP_NODELAY attestation, two distinct
co-located HPX localities, and an on-driver-node Ray actor — each arm summarized separately.

**Does NOT establish / claim:** no “HPX beats Ray” / “Ray beats HPX”, no winner, no speedup, no ratio,
no same-node-vs-cross-node ratio, no production / fault-tolerance / inference / scaling claim, and no
implication that the shipped `rayx.runtime` API gained distributed actions. **Slice 4 (cross-node)
remains the current best cross-node same-axis evidence; Slice 5 is the same-node control.**
