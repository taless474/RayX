# exp59 — Ray Actor Call Path vs HPX Action Path (closed-int64 micro-workload)

**Status:** Slice 0 plan/schema **done**. Slice 1 same-host Ray actor control **implemented and
validated** (medusa00, `--phase same-host-control`). Slice 2 two-node Ray placement proof **PASSED on
hardware** (Rostam job `158680`, node pair medusa00/medusa01, `overall: pass`, 20/20 pass gates — see
§3b). Slice 3 R=1 two-node measurement **PASSED on hardware** (`--phase two-node-measurement-r1`, Rostam job
`158681`, medusa00/medusa01, `overall: pass`, 24/24 gates — see §3c). Slice 4 R=5 replicated measurement
**implemented** (`--phase two-node-measurement-r5`, one cluster, fresh actors + full placement gates per
island, per-island-primary/no-pooling — see §3d); awaiting a 2-node Rostam run. Still no HPX comparison,
no perf claim, no failure/restart.

**One-line framing.** Characterize the **native Ray actor call path** for a tiny closed-`int64`
workload and place it next to the **Ray-supervised HPX remote-action path** characterized in exp58.
This is **path characterization of one micro-workload under one Rostam allocation**, not a general
Ray-vs-RayX or Ray-vs-HPX performance claim.

**Roadmap context (new).** exp58 = clean-path HPX performance characterization (done). **exp59 = Ray
actor baseline vs HPX action path (this).** exp60 = whole-island failure/restart.

---

## 1. Experiment objective

**Measure:**
- Native Ray actor call-path overhead for a tiny closed-`int64`-style workload (caller actor →
  remote actor method → returned value + cross-node proof).
- Compare that path to the HPX action path characterized in exp58 (the Ray-supervised HPX remote
  `dist_probe` over the TCP parcelport).
- **Separate startup/control-plane timing from steady-state call timing** (Ray cluster bring-up,
  init/connect, actor creation/placement, shutdown vs the QD1 / pipeline call loop).
- Preserve correctness gates and **raw per-call timing arrays**.

**Must NOT claim:**
- No general Ray-vs-RayX claim.
- No "HPX beats Ray" claim.
- No production/API claim.
- No network/fabric performance claim.
- No broad benchmark claim.
- No failure/restart claim (that is exp60).

---

## 2. Fairness framing (this is NOT a clean apples-to-apples runtime comparison)

State explicitly and prominently in every artifact:
- The **HPX path** is a C++ HPX action over the TCP parcelport (HPX parcel/action/future
  semantics), measured on the root HPX thread.
- The **Ray path** is a native Ray actor method call, **Python-level** unless a different
  implementation is explicitly chosen and labeled. Ray actor calls include Ray's scheduling,
  (de)serialization, object-ref/result-retrieval, and Python-boundary semantics.
- The two paths bundle **different layers**: Python driver + Ray runtime + gRPC/object store vs C++
  HPX runtime + parcelport. A raw ns difference is **not** a runtime-quality verdict.
- **The only fair claim** is about *this closed-`int64` micro-workload under the same (or
  comparable) Rostam conditions*: "the Ray actor call path measured higher/lower than the HPX action
  path for this micro-workload, on this allocation." Nothing broader.

This caveat block must be copied into `ray_actor_aggregate.json` and `comparison_aggregate.json`
(e.g. a `fairness_caveats` array), not just the write-up.

---

## 2a. Measurement plane & cross-runtime asymmetry (HPX-internals review hardening)

The biggest risk is presenting two QD1 floors **measured at different planes** as if they share an
axis. The exp58 HPX floor is measured *inside `hpx_main` on an HPX worker thread* (C++ `steady_clock`)
— it includes future suspend/resume but **excludes any Python/driver boundary**. The exp59 Ray floor
is measured *in the Python driver (or a pinned Python caller actor)* around `ray.get(...)` — it
includes Python dispatch, driver→raylet→actor→object-store→driver, and Python deserialize. These are
**different measurement points**. The schema must carry that difference, not just the prose.

### Measurement-point fields (both sides)

HPX reference side (`hpx_reference_aggregate.json`, carried from exp58):
```
measurement_point="hpx_worker_thread"
measurement_plane="runtime_internal_cpp"
python_boundary_included=false
driver_observed=false
future_suspend_resume_included=true
```

Ray actor side (`ray_actor_aggregate.json`):
```
measurement_point                  # "python_driver" | "pinned_python_caller_actor"
measurement_plane                  # "python_driver_observed" | "python_actor_observed"
python_boundary_included=true
driver_observed                    # true (driver caller) | false (pinned caller actor)
ray_driver_to_raylet_path_included # true (driver caller) | false (intra-actor caller)
ray_get_included=true
```

> **Write-up requirement.** The exp58 HPX QD1 number is a **runtime-internal C++ action floor**; the
> Ray QD1 number is **Python/Ray-observed** (driver-observed unless a pinned caller actor is used).
> They are comparable only as **path characterizations**, not as identical measurement planes.

### HPX comparator choice (point 2)
exp59 initially compares the Ray actor path against the **exp58 in-substrate C++ HPX action floor** —
the cleanest available HPX path, but it **excludes** the Python→HPX boundary cost that the Ray path
pays.
```
hpx_comparator_kind="in_substrate_cpp_action_floor"
hpx_python_boundary_included=false
python_boundary_asymmetry_disclosed=true
```
> **Future work.** A **Python-boundary-inclusive** HPX/RayX endpoint comparator (a `rayx`/endpoint
> Python call path vs a Ray Python call path) is a *separate future experiment* if we want
> user-facing-Python vs user-facing-Python. exp59 does not attempt it.

### Ray object-store / result-path asymmetry (point 3)
A Ray actor call pulls its result through the object store; HPX has **no** object-store analog.
```
ray_object_store_in_result_path=true
ray_result_retrieval="ray.get"
ray_result_serialization_path_included=true
hpx_object_store_in_result_path=false
```
> **Write-up requirement.** Ray actor calls include Ray result-retrieval / object-ref semantics; HPX
> action calls have no Ray object-store analog. This is a structural category difference, not a tuning
> knob.

### Same-host Ray decomposition control (point 8)
Analogous to the exp58 loopback control (which would separate HPX parcel-stack cost from the network
leg), a **same-node caller/callee Ray actor run** separates Ray software-path overhead from the
inter-node leg. Optional, but reserved in the schema.
```
same_host_ray_control_available
same_host_ray_control_run
same_host_ray_control_purpose
same_host_ray_control_result_ref   # pointer to the same-host run_aggregate, when run
```
> The same-host control is a **decomposition row**, not the headline number; when run, its QD1 floor
> is recorded so the inter-node Ray number is decomposable rather than a lump.

---

## 3. Topology

Two-node Ray topology analogous to exp58:
- **nodeA = caller/root side** — the Ray driver and the *caller* actor (or the driver itself issues
  calls). Maps to exp58 `medusa00`.
- **nodeB = callee/worker side** — the remote `Probe` actor that executes the closed-`int64` method.
  Maps to exp58 `medusa01`.
- The caller issues repeated `.remote()` calls to the remote actor; the remote actor executes the
  closed-`int64` method and returns the value **plus proof metadata**.
- Record on every call (or at least first/last + a sampled subset): callee `hostname`, `pid`, Ray
  `node_id`, actor id — to **prove cross-node execution** (nodeB, not nodeA).

### Does this need a real two-node Ray cluster under Slurm?
**Yes — and the plan must not assume local Ray can place actors on `medusa00`/`medusa01`.** Ray
placement across physical nodes requires a real multi-node Ray cluster; a single-host `ray.init()`
cannot put an actor on a different physical node. So cross-node proof is a **hard gate**, not an
assumption. Until Slice 2 proves placement, no cross-node Ray number is meaningful.

### Intended safe Slurm launch shape (to be implemented in Slice 2, not now)
1. Allocate two nodes (same shape as exp58: `salloc -p medusa -N2 --exclusive`).
2. **Ray head on nodeA**: `ray start --head --node-ip-address <A_ip> --port <p> ...` (pin the
   dashboard off, bounded object-store memory).
3. **Ray worker on nodeB**: `ray start --address <A_ip>:<p> --node-ip-address <B_ip> ...` (issued via
   `srun -N1 -n1 --nodelist=nodeB`, `--export=ALL`, pre-Ray-anchored env, GCC-15 ldd discipline
   carried over from exp57/58 if the Ray worker also loads the HPX-comparable toolchain — not
   strictly required for the pure-Ray actor path, but recorded).
4. **Driver connects** with `ray.init(address="auto"|<A_ip>:<p>)`.
5. **Pin actors to nodes — prefer HARD affinity.** Use
   `NodeAffinitySchedulingStrategy(node_id=<resolved>, soft=False)` when available (hard placement).
   Custom node resources (`ray start ... --resources '{"nodeB":1}'` + `Probe.options(resources=...)`)
   are an acceptable fallback, but fractional custom resources (`0.001`) can silently co-schedule, so
   if used they must still be verified against resolved `node_id`/hostname. Record the chosen
   mechanism and **verify every (or sampled + first/last) call's callee node_id/hostname**.
6. **Clean stop**: `ray.shutdown()` on the driver, then `ray stop` on both nodes via `srun`.
7. **No-orphan check**: `pgrep -f raylet|gcs_server|plasma|ray::` on both nodes returns empty
   (mirrors exp58 `_orphan_check_node`).

If a reliable two-node Ray cluster cannot be stood up in the allocation, exp59 **stops at a clean
skip** (placement gate fails) rather than reporting same-host numbers as cross-node.

### Placement-strategy fields (point 5) — a single off-node sample fails the run
```
placement_strategy                 # "node_affinity_hard" | "custom_node_resources" | ...
placement_soft                     # false required for the hard-affinity path
caller_resolved_node_id
callee_resolved_node_id
caller_hostname
callee_hostname
cross_node_placement_verified      # callee != caller, on intended nodeB
off_node_sample_count              # number of sampled calls that executed off nodeB
off_node_sample_gate_passed        # true iff off_node_sample_count == 0
```
> A single off-node sample is a **gate failure**, not an average — `off_node_sample_gate_passed` must
> be `true` for the run to be valid.

### Caller location (point 6) — decide and record
**Recommendation: use a pinned caller actor on nodeA** if practical, because it mirrors exp58
(caller on nodeA, callee on nodeB, driver only orchestrates) and lets the QD1 floor be measured at a
`pinned_python_caller_actor` plane rather than a `python_driver` plane. Driver-as-caller is simpler
to start with; if used, record it and set the measurement plane accordingly (and note the driver's
node is wherever Slurm placed it, which may not be nodeA).
```
caller_role                        # "driver" | "pinned_caller_actor"
caller_is_driver                   # true | false
caller_is_pinned_actor             # true | false
driver_hostname
driver_node_id
caller_hostname
caller_node_id
```

---

## 3a. Slice 2 placement-proof hardening (review-driven; HPX-comparison honesty)

These requirements harden the Slice 2 two-node placement proof so (a) the cross-node proof is
*structurally* strong rather than a hostname-string compare, and (b) the eventual HPX comparison
(Slice 5 vs exp58) stays honest on node-pair / interface / measurement-plane parity. They are
**plan/schema only**; Slice 2 remains placement proof with **no perf, no HPX comparison, no
failure/restart**.

### (1) Value-encoded node proof — the oracle value must carry the callee node
Slice 1 constructs `ProbeActor(node_tag=1)`, so the closed-`int64` value `(x ^ PROBE_XOR) +
(node_tag<<1)` is **constant regardless of which physical node executes it**. Slice 2 instead assigns
`node_tag` **by target node** (nodeA→1, nodeB→2) so the callee the caller *intends* for nodeB carries a
distinct value from the caller's own. **Important precision:** this `node_tag` is a **driver-assigned
constructor argument, not sampled from the executing node** — so, unlike exp57/58's
`(x ^ RAYX) + (loc<<1)` where `loc` is read from the executing locality, the value here confirms that
the *intended callee actor executed and returned correctly*; it does **not by itself** bind execution
to nodeB. Physical node placement is bound by Ray `node_id` + FQDN-normalized hostname (below).

```
node_tag_assignment                # e.g. {"nodeA": 1, "nodeB": 2}
caller_node_tag                    # nodeA tag (e.g. 1)
callee_node_tag                    # nodeB tag (e.g. 2)
expected_callee_node_tag           # the nodeB tag the caller expects
value_encoded_node_proof=true
```

The callee actor pinned to nodeB **must be constructed with nodeB's tag**, and the proof call must
verify **all** of:
- `result == (x ^ PROBE_XOR) + (expected_callee_node_tag << 1)`;
- returned `node_tag == expected_callee_node_tag`;
- callee `hostname` / `node_id` matches the intended nodeB.

> **Write-up requirement (claim precision).** The Slice 2 placement proof is the **combination** of
> hard `NodeAffinitySchedulingStrategy(soft=False)`, Ray `node_id` resolution (authoritative),
> FQDN-normalized hostname match, **and** oracle correctness. Ray `node_id` + hostname prove *where* the
> actor ran; the value/oracle proves the *intended callee actor executed correctly and returned the
> expected closed-`int64`*. No single one of these — least of all the driver-assigned `node_tag` value
> alone — is claimed to prove nodeB execution on its own.

### (2) Ray node-id resolution gate — resolve Slurm node → Ray node_id *before* actor creation
Ray `node_id`s are opaque hex; mapping a Slurm hostname to the right one is the single most
bug-prone step. After cluster startup and **before any actor is created**, enumerate `ray.nodes()`
and match each **alive** Ray node to the intended Slurm node using `NodeManagerAddress`, hostname /
`NodeName` if available, and the resolved selected-subnet IP.

```
ray_nodes_raw                      # the ray.nodes() snapshot used for resolution
ray_node_resolution_method         # how NodeManagerAddress/NodeName/subnet-IP were matched
nodeA_ray_node_id
nodeB_ray_node_id
nodeA_ray_node_ip
nodeB_ray_node_ip
nodeA_ray_node_match_count
nodeB_ray_node_match_count
ray_node_id_resolution_ok
ray_nodes_on_selected_subnet       # true/false
```

**Gate (fails before actor creation):**
- exactly one alive Ray node matches nodeA (`nodeA_ray_node_match_count == 1`);
- exactly one alive Ray node matches nodeB (`nodeB_ray_node_match_count == 1`);
- both matched Ray node IPs fall inside `selected_subnet`;
- zero or ambiguous matches → hard fail (no actors created).

### (3) Deterministic node-pair role assignment
Slurm nodelist order is not guaranteed, so role assignment must be deterministic and recorded so
reruns are stable and nodeA consistently maps to exp58's `medusa00` (root) role.

```
node_pair_selection_rule           # e.g. "sort nodelist; first=nodeA/caller/head, second=nodeB/callee/worker"
matches_exp58_node_pair
expected_exp58_nodeA="medusa00"
expected_exp58_nodeB="medusa01"
node_pair_parity_with_exp58
```

Rule: sort the Slurm nodelist deterministically; assign the lexicographically-first node as
**nodeA/caller/head** (expected `medusa00` when the allocation is `medusa[00-01]`) and the second as
**nodeB/callee/worker** (expected `medusa01`). If the allocation differs from medusa00/medusa01,
**do not fail the placement proof** — but set `node_pair_parity_with_exp58=false` and **prohibit any
later direct exp58 comparison** unless the HPX side is recaptured in the same allocation (Option B).

### (4) Interface / subnet parity gate
exp58's HPX path rode `eno16` / `10.42.5.x`. Slice 2 must resolve Ray node IPs on the **same selected
subnet** intended for comparison, and record whether Ray actually bound that interface.

```
selected_subnet
expected_interface="eno16"         # when selected_subnet == "10.42.5."
ray_node_ips_on_selected_subnet
interface_parity_with_exp58
```

If Ray binds a different interface/NIC than exp58's parcelport leg, the placement proof may still be
informative, but `interface_parity_with_exp58` must be **false** (the later comparison cannot claim
same-interface parity).

### (5) Placement aggregate must be timing-free
Slice 2 is placement proof only. The file already contains the Slice 1 Class-B (`_run_measurement`)
machinery; Slice 2 must **not** reuse it, and the placement aggregate must not carry QD1/pipeline
timing arrays — only tiny proof-call metadata plus control-plane startup/shutdown facts. This keeps
the "no perf" fence load-bearing in the artifact, so nobody downstream reads a same-allocation
placement call as a cross-node latency floor.

```
timing_measured=false
class_b_timing_present=false
perf_claim_allowed=false
```

### (6) Orphan check pattern (carry exp57/58 `_orphan_check_node` discipline)
Check both nodes after `ray stop` for at least: `raylet`, `gcs_server`, `plasma_store`, `ray::`, and
`dashboard`/`monitor` if present.

```
orphan_check_patterns              # e.g. ["raylet","gcs_server","plasma_store","ray::","dashboard","monitor"]
orphan_ray_processes_nodeA
orphan_ray_processes_nodeB
no_orphan_ray_processes_nodeA
no_orphan_ray_processes_nodeB
```

### (7) Driver-node honesty
Under Slurm the driver runs wherever `python` was launched (possibly the login node, as bit Slice 1's
first run). The **caller plane is the pinned caller actor on nodeA**; driver placement is recorded but
does not define the measured caller plane, so a stray login-node driver cannot be misread as caller
placement.

```
driver_hostname
driver_node_id
driver_is_on_slurm_node
caller_plane_defined_by="pinned_caller_actor"
driver_not_in_caller_plane=true
```

### (8) Updated Slice 2 pass gates
All of the following must hold for a valid Slice 2 run:
- Slurm two-node allocation present;
- Ray head on nodeA and worker on nodeB start cleanly;
- exactly one Ray node matches nodeA;
- exactly one Ray node matches nodeB;
- Ray node IPs are on the selected subnet;
- hard `NodeAffinitySchedulingStrategy` used with `soft=False`;
- caller actor resolves to nodeA Ray node id;
- callee actor resolves to nodeB Ray node id;
- value-encoded node proof matches the nodeB tag;
- caller and callee hostnames differ;
- caller and callee Ray node IDs differ;
- `off_node_sample_count == 0`;
- `ray stop` succeeds on both nodes;
- no orphan Ray processes on both nodes;
- `timing_measured == false`.

> A single off-node sample, a soft-placement fallback, an ambiguous node-id match, or any orphan
> process is a **gate failure**, not an average.

### (9) Claim fences (reiterated for Slice 2)
Slice 2 licenses only **two-node Ray placement proof**: no performance claim, no HPX comparison, no
Ray-vs-HPX claim, no production/API claim, no failure/restart claim. A failed placement/parity gate
yields a clean skip/fail aggregate (redirected by the writer), never a same-host or off-parity number
masquerading as a comparable cross-node placement.

---

## 3b. Slice 2 result — PASS on hardware (Rostam job 158680)

Slice 2 **passed on hardware** on a two-node Rostam allocation (Ray 2.55.1).

- **Rostam job id:** `158680`
- **Node pair:** medusa00 / medusa01 (matches the exp58 pair; eno16 / `10.42.5.x`)
- **Overall:** `pass` — **20/20 pass gates true**

**What was proven (placement proof only):**

- **Caller actor hard-placed on nodeA (medusa00)** and **callee actor hard-placed on nodeB
  (medusa01)** via `NodeAffinitySchedulingStrategy(soft=False)`.
- Placement asserted by **Ray `node_id`** (authoritative) **plus FQDN-normalized hostname checks** —
  actors report `medusaNN.rostam.cct.lsu.edu`; locality is decided by `node_id` first and a
  short-name-normalized hostname as a secondary check.
- **Oracle correctness passed** (`proof_call_correct = true`; oracle `1380014435`) — this confirms the
  *intended callee actor executed and returned the expected closed-`int64`*; node placement itself is
  bound by `node_id` + hostname above, not by the (driver-assigned) value alone. The proof is the
  **combination** of hard NodeAffinity, Ray `node_id`, FQDN-normalized hostname, and oracle correctness.
- **`off_node_sample_count = 0`.**
- **Clean Ray teardown:** `ray stop --force` succeeded on both nodes.
- **Zero orphan Ray processes** on both nodes.

> **Not workload timing.** The artifact's readiness fields (`gcs_ready_wait_s`, `ray_init_wait_s`,
> `ray_nodes_ready_wait_s`) are **orchestration/startup diagnostics**, not a workload latency
> measurement. Slice 2 records `timing_measured=false`; no per-call timing exists in this phase.

**Architecture — orchestrator / inner-proof split (what made the driver attach work):**

- The **orchestrator on rostam1 owns Ray process lifetime**: it starts the **Ray head (nodeA)** and
  **Ray worker (nodeB)** as **`--block` `subprocess.Popen` srun steps**, runs the readiness gates and
  secondary-port pinning, and guarantees teardown (`ray stop --force`, launcher termination, no-orphan
  checks) under one `try/finally`.
- The **proof itself runs on nodeA** through an internal **`--phase _two-node-inner-proof`** step
  launched with a sibling `srun --nodelist nodeA` (structured JSON handed back via files on the shared
  `/work` filesystem). A Ray driver must run from a Ray cluster node; the rostam1 orchestrator can
  TCP-reach GCS but is not a raylet node, so the inner proof is co-located with the head raylet.

**Process note (provenance):**

- Job `158679` first **exposed an FQDN vs short-hostname bug**: placement was actually correct (by Ray
  `node_id`), but the gate compared the actor's FQDN `medusa01.rostam.cct.lsu.edu` against the short
  Slurm name `medusa01`, falsely counting every sample as `off_node`.
- Job `158680` **confirmed PASS** after the hostname comparison was normalized.

**Claim fences (unchanged):** Slice 2 licenses only **two-node Ray placement proof** — **no timing, no
HPX comparison, no Ray-vs-HPX claim, no performance claim, no failure/restart claim**. Closed-`int64`
micro-workload, Rostam-allocation-specific, parity gated to medusa00/medusa01.

### Ray-on-Slurm operational lessons

- Ray head/worker need **`--block`-style lifetime ownership** under orchestration; otherwise Slurm
  reaps the srun step's process group and kills GCS/raylet.
- The Ray **driver/proof must run from a Ray cluster node** for this placement-proof path; TCP
  reachability to GCS is necessary but **not sufficient** to attach a driver.
- **Hostname comparisons must normalize FQDN vs short names** — assert locality by `node_id` first and
  treat hostname as a normalized secondary check.
- **Artifact copy-back should avoid clobbering source** (`rsync --exclude '*.py'`) unless the intent is
  to deliberately sync code.
- **Secondary Ray port pinning:** the `--node-manager-port` / `--object-manager-port` /
  `--min-worker-port` / `--max-worker-port` flags were **accepted by Ray 2.55.1** (head and worker came
  up with them pinned). Dashboard / runtime-env / agent ports were **deliberately not guessed** — their
  `ray start` flag names vary across Ray versions and an unrecognized flag aborts `ray start --block`.

---

## 3c. Slice 3 — R=1 two-node Ray actor measurement (implemented; awaiting Rostam run)

New phase **`--phase two-node-measurement-r1`**, on the **same** orchestrator(rostam1)/inner(nodeA)
split as Slice 2.

- **Placement-gated:** it runs the **identical Slice 2 placement gates first** (node-id resolution,
  `soft=False`, value oracle, FQDN-normalized hostname, `off_node==0`, no-orphan, parity). **If any
  placement gate fails, no workload timing is recorded.**
- **Measurement (only after placement passes):** an R=1 QD1 Class-B band measured **inside** the pinned
  `CallerActor` on nodeA against the `ProbeActor` on nodeB — warmup `W` (default 100), measured `K`
  (default 1000), monotonic-ns per call (`time.perf_counter_ns`), `p50/p90/p99/mean/min/max` + a
  correctness count; plus an optional pipeline sanity row (`--measure-depths`, default `8,32,128`).
  Reuses the exact Slice-1 `_run_measurement`.
- **Honest plane fields:** `measurement_point = pinned_caller_actor_nodeA_to_callee_actor_nodeB`;
  `measurement_plane = ray_python_ray_actor_observed_path (NOT hpx_cpp_runtime_internal)`.
- **Separate artifact:** writes `ray_actor_two_node_measurement_r1_aggregate.json`; the Slice 2
  placement aggregate is **not** touched. Sets `timing_measured=true` / `class_b_timing_present=true`
  **only after placement passes and the band is recorded**, plus `r_count=1`, `not_r5=true`,
  `not_hpx_comparison=true`, `measurement_plane_asymmetry=true`, `perf_claim_allowed=false`,
  `hpx_comparison_performed=false`.
- **Claim fences:** R=1 single-allocation **path band only** — Ray Python/Ray-actor-observed plane,
  explicitly **not** the exp58 HPX C++ runtime-internal floor and **not** on the same axis. **No HPX
  comparison, no R=5, no Ray-vs-HPX, no performance verdict, no failure/restart.**

---

## 3d. Slice 4 — R=5 replicated two-node Ray actor measurement (implemented; awaiting Rostam run)

New phase **`--phase two-node-measurement-r5`** (`--measure-islands`, default `5`), on the **same**
orchestrator(rostam1)/inner(nodeA) split as Slices 2–3.

- **Design choice (confirmed): one Ray cluster for the whole run; fresh caller/callee actors per
  island.** The cluster is stood up once; each of the R islands creates a **new** hard-pinned
  caller@nodeA / callee@nodeB pair (`ray.kill`ed at island end so the next island places fresh),
  isolating call-path jitter rather than folding in per-island cluster bring-up variance.
- **Per-island placement gate:** every island re-runs the **identical Slice 2 placement proof**
  (node-id resolution reused at cluster level; `soft=False`, value oracle, FQDN-normalized hostname,
  `off_node==0`) **before** its QD1 band is timed. An island that fails placement records **no timing**
  for itself; `overall=pass` requires **every** island valid (`all_islands_valid`), otherwise the run
  is an **honest partial/fail** (valid islands keep their bands; `timing_measured=true` once any island
  measured, with `overall=fail`).
- **Statistics — per-island primary, no pooling.** Each island keeps its own QD1 Class-B band; the
  aggregate carries `across_island_stats` = the **median** of per-island `p50/p90/p99/mean/min/max`
  plus the across-island **min/max spread**. Calls are **not** pooled into one distribution
  (`pooled_distribution_used=false`, `per_island_primary=true`).
- **Pre-registered decision rule** (carried in `across_island_stats.decision_rule`): *a gap inside the
  across-island jitter band is **not** a separable effect* — the spread bounds what the median can claim.
- **Separate artifact:** writes `ray_actor_aggregate.json` (the §9 cross-node name); the Slice 2 and
  Slice 3 aggregates are **not** touched. Sets `r_count=5`, `per_island_primary=true`,
  `pooled_distribution_used=false`, `not_hpx_comparison=true`, `measurement_plane_asymmetry=true`,
  `perf_claim_allowed=false`, `hpx_comparison_performed=false`; `timing_measured=true` only when valid
  island timings exist.
- **Claim fences:** R=5 replicated **path bands only** — Ray Python/Ray-actor-observed plane,
  explicitly **not** the exp58 HPX C++ runtime-internal floor and **not** on the same axis. **No HPX
  comparison, no Ray-vs-HPX, no performance verdict, no failure/restart.**

---

## 3e. Slice 5 — Ray↔HPX plane-labeled juxtaposition + within-runtime decompositions (PASS / final)

Slice 5 is the **comparison-gating** slice. It does **not** measure anything new: it juxtaposes the
four already-passing R-banded sources below, **labels the measurement planes**, and reports **two
within-runtime decompositions** that are **never crossed**. Curated artifact:
**`ray_vs_hpx_plane_labeled_aggregate.json`** (hand-curated; all percentiles copied read-only from the
sources; the only arithmetic is each runtime's own same/cross increment).

### The four sources (QD1 only)

| plane | rung | run | p50 | p90 | p99 |
|---|---|---|---|---|---|
| Ray — Python/`ray.get`-observed | same-host (control) | exp59 Slice 1, **R=1** | ~609 µs | ~651 µs | ~733 µs |
| Ray — Python/`ray.get`-observed | cross-node | exp59 Slice 4, **R=5** | ~742 µs | ~1056 µs | ~1190 µs |
| HPX — caller-observed C++ `async().get()` | same-node TCP (loopback) | exp60, **R=5** | ~76.6 µs | ~86.8 µs | ~101.8 µs |
| HPX — caller-observed C++ `async().get()` | inter-node TCP (eno16) | exp58, **R=5** | ~115.8 µs | ~153.8 µs | ~185.7 µs |

(exp60 reuses the **exp58 binary unmodified**, so the two HPX rungs share one measurement core; both
HPX rungs and both Ray-cross-node bands are per-island-primary, nearest-rank, median-of-medians.)

### 1. Plane-labeled juxtaposition only

Two different rulers, **not** two runtimes on one axis:

- **Ray plane** = Python/Ray-actor-observed `ray.get` RTT — Python frame + core-worker gRPC IPC +
  object-ref ownership/`ray.get` + cross-node transport + remote exec + result fetch back to Python.
- **HPX plane** = caller-observed C++ `hpx::async(...).get()` RTT on the root locality — future
  suspend/resume + TCP parcel serialization + transport, with **no** Python boundary and **no**
  object-ref ownership protocol.

There is therefore **no ratio, no speedup, and no same-axis comparison** — none is computable, and none
is reported. (The Ray per-call floor is **not** cloudpickle, which warmup amortizes, and **not** plasma,
since closed-`int64` is inlined — it is the Python boundary + core-worker IPC + `ray.get` path.)

### 2. Two within-runtime decompositions (each stays inside its own runtime)

- **Within Ray:** same-host ~609 µs of cross-node ~742 µs → **cross-node increment ≈ 133 µs**
  (`741661 − 608695` ns). ~82 % of the Ray QD1 p50 floor is present with **no network**. *Caveat: the
  same-host control is **R=1** (indicative, not a band), so the increment is approximate.*
- **Within HPX:** same-node TCP ~76.6 µs of inter-node TCP ~115.8 µs → **wire increment ≈ 39 µs**
  (`115831 − 76625` ns; p99 wire increment ≈ 84 µs). ~66 % of the HPX QD1 p50 floor is local stack +
  TCP-over-loopback software; only ~39 µs is the eno16 wire. *Caveat: kernel loopback ≠ zero cost;
  `tcp_nodelay` unverified → Nagle held **constant** across L1/L2, not eliminated.*

These two increments are **each within their own runtime** and are **never subtracted or ratioed
across runtimes**. The shared finding is qualitative and per-runtime: **in each runtime the QD1 floor
is dominated by local stack, not network.**

### 3. Not same-axis (restated)

The Ray ~742 µs and HPX ~115.8 µs cross-node p50s measure **different layers**: a user-facing
Python actor call vs an in-runtime C++ caller RTT. The bulk of the cross-runtime gap is the
**plane/layer difference** (Python boundary + `ray.get` path), evidenced by the Ray same-host floor
(~609 µs) persisting with zero network. This gap is **not** evidence about transport or runtime
quality.

### 4–7. Fences honoured in this section

- **No speedup, no ratio** computed or reported (§1).
- **No “HPX beats Ray.”** The HPX numbers are runtime-internal C++ caller RTT, explicitly **not**
  user-facing Python call-path numbers.
- **Placement wording:** Ray placement is proven by **hard `NodeAffinity(soft=False)` + resolved Ray
  `node_id` + FQDN-normalized hostname** (caller@nodeA ≠ callee@nodeB, `off_node==0`). **Oracle
  correctness proves the intended actor executed and returned the expected closed-`int64`** — it does
  **not**, by itself, prove physical node placement.
- **Pipeline excluded:** QD8/QD32/QD128 exist on both sides (Ray object-refs + `ray.get(list)` vs HPX
  `wait_all` over futures) but are **out of scope** — different batching mechanisms; amortized
  makespan/N is not a latency.

### Slice 5 verdict

`comparison_kind = plane_labeled_juxtaposition_only`, `same_axis_comparison = false`,
`speedup_computed = false`, `ratio_reported = false`, `verdict_allowed = false`,
`perf_claim_allowed = false`. Source provenance (paths + run_ids + sha256 prefixes + percentiles) is
recorded in `source_artifacts`. **exp59 is complete through Slice 5.**

---

## 4. Workload (Ray actor method)

Deterministic, closed-`int64`, same oracle *spirit* as exp58 (it need not reproduce HPX locality
ids, but must give analogous cross-node proof):

```
class Probe:                       # Ray actor, one per role (callee on nodeB)
    def dist_probe(self, x: int) -> dict:
        # closed int64: result = (x ^ 0x52415958) + (node_tag << 1)   [node_tag derived locally]
        return {
            "result": <int64>,
            "hostname": socket.gethostname(),
            "pid": os.getpid(),
            "ray_node_id": ray.get_runtime_context().get_node_id(),
            "actor_id": ray.get_runtime_context().get_actor_id(),
        }
```

- **Oracle**: caller knows `x` and the callee's `node_tag` (resolved once at placement time), so it
  can verify `result` deterministically — analogous to the exp58 `(x ^ RAYX) + (loc<<1)` check.
- **Cross-node proof** (the analog of exp58's "remote locality differs"): callee `hostname`/`pid`/
  `ray_node_id` ≠ caller's, and the placement gate confirmed nodeB.
- Keep payload tiny (one int + small dict) so the measurement is call-path overhead, not data
  movement. (A pure-`int` return variant may be added to isolate dict-serialization cost; record
  which variant was used.)

---

## 5. Timing design (mirror exp58 structure)

High-resolution timing on the Ray (Python) side:
- `time.perf_counter_ns()` for the actor path; record `clock_type="time.perf_counter_ns"` and the
  clock resolution if discoverable (`time.get_clock_info("perf_counter")`).
- Raw per-call durations recorded in **ns**; also aggregate loop duration + aggregate-derived mean
  (cross-check vs per-call mean, as in exp58).
- Estimate timestamp-call overhead (`timestamp_overhead_ns`) the same way exp58 does.

**Modes (same shape as exp58):**
- **Prewarm**: 1 deliberate call (one-shot; may include first-call / connection / scheduling warmup)
  → `prewarm_call_duration_ns`.
- **Warmup**: `W` calls, dropped from steady-state; first warmup → `first_call_duration_ns`.
- **Depth-1 QD1 serialized floor**: `K` serialized `ray.get(actor.dist_probe.remote(x))` calls.
  - metric name: **`actor_call_rtt_floor_depth1`**
  - alias: **`ray_actor_closed_int64_call_overhead_floor_qd1`**
  - "serialized QD1 call-path floor; **not** general per-call cost" note (exp58-style).
- **Pipeline**: issue `N` `.remote()` calls without per-call waiting, collect with
  `ray.get(futures)`, at depths `[8, 32, 128]`.
  - report `pipeline_actions_per_sec` and `pipeline_amortized_action_time_ns`
    (**not a latency** — makespan/N, tail-gated; carry the exp58 note verbatim in spirit).

**Defaults:** `K=1000`, `W=100`, `R=5` islands, pipeline depths `[8, 32, 128]`.

### Class-B schema (per island, Ray path)
```
clock_type                         # "time.perf_counter_ns"
clock_resolution_ns
timestamp_overhead_ns
prewarm_call_duration_ns
first_call_duration_ns
actor_call_rtt_floor_depth1:
  metric_name, alias, queue_depth=1, K, W, steady_count, correct_count,
  per_call_duration_ns_raw[K], aggregate_loop_duration_ns, aggregate_mean_call_ns,
  min_ns, mean_ns, p50_ns, p90_ns, p99_ns, max_ns, note
remote_action_pipeline: [           # one per depth in [8,32,128]
  { queue_depth, pipeline_actions, pipeline_total_duration_ns, pipeline_actions_per_sec,
    pipeline_amortized_action_time_ns, pipeline_correct_count, pipeline_remote_proof_first_last,
    ray_get_primitive="ray.get",
    pipeline_batching_mechanism="ray_object_refs_plus_ray_get_list",  # Ray side
    note }
]
timing_loop_context                # "python_driver" (NOT an HPX thread; recorded honestly)
remote_id_cached                   # actor handle cached once and reused (analog of exp58 cached id)
per_iteration_placement_lookup=false
```

### Pipeline batching disclosure (point 4)
The two pipelines drain differently, so `pipeline_actions_per_sec` is **not the same kind of
throughput** on both sides:
```
# Ray side (ray_actor_aggregate.json)
pipeline_batching_mechanism="ray_object_refs_plus_ray_get_list"   # or "ray_object_store_batched_get"
# HPX side (carried from exp58)
pipeline_batching_mechanism="hpx_parcel_coalescing"
# comparison side (comparison_aggregate.json)
pipeline_cross_runtime_ratio_allowed=false   # unless BOTH batching mechanisms are disclosed
pipeline_ratio_note
```
> **Write-up requirement.** Depth-128 pipeline throughput must **not** be read as the same kind of
> throughput on both sides unless batching mechanisms are explicitly disclosed: the HPX pipeline may
> include **parcel coalescing**; the Ray pipeline may include **object-ref batching / `ray.get(list)`**
> behavior. A cross-runtime pipeline ratio is allowed only when
> `pipeline_cross_runtime_ratio_allowed=true` (both mechanisms disclosed) and labeled as a
> micro-workload path ratio.

### Warmup sufficiency (point 7) — carry exp58 cold-path decomposition
```
prewarm_call_duration_ns
first_warmup_call_duration_ns
steady_state_p50_ns
warmup_sufficiency_note
warmup_sufficiency_gate            # "ok" if prewarm & first warmup are clearly > steady_state_p50,
                                   # else "uncertain"
```
> Prewarm + `W=100` warmups should absorb actor spin-up, Python import, connection-pool
> establishment, cloudpickle, and object-ref warmup. The run reports whether `prewarm_call_duration_ns`
> and `first_warmup_call_duration_ns` are clearly **above** `steady_state_p50_ns`; otherwise warmup
> sufficiency is marked **uncertain** (do not silently fold residual warmup into steady-state).

---

## 6. Startup / control-plane timing (recorded SEPARATELY from Class-B)

```
control_plane:
  ray_cluster_head_start_ms        # ray start --head wall time
  ray_cluster_worker_start_ms      # ray start --address on nodeB
  ray_init_connect_ms              # ray.init(address=...) on the driver
  actor_creation_ms                # Probe.options(...).remote()
  actor_placement_resolve_ms       # time to confirm node_id/hostname == nodeB
  first_call_prewarm_ms            # the prewarm call (also ns in Class-B; here as control context)
  ray_shutdown_ms                  # ray.shutdown() + ray stop on both nodes
  no_orphans                       # pgrep raylet|gcs|plasma empty on both nodes
```

These are **Class-A / control-plane** and must never be mixed into the Class-B QD1 / pipeline
numbers (same discipline exp58 used to keep `srun` issue timing out of the action timing).

---

## 7. HPX comparison source — recommendation

**Recommended: Option A for the first exp59 comparison, with Option B reserved as an optional
same-allocation HPX recapture.**

- **Option A (recommended now):** consume the exp58 curated R=5 aggregates as the established HPX
  reference:
  - `experiments/58_two_node_clean_path_perf/perf_aggregate_ray_supervised.json` (primary HPX
    reference — same Ray-as-control-plane supervision shape).
  - `experiments/58_two_node_clean_path_perf/perf_aggregate_rayfree.json` (context only).
  exp58 already validated these at R=5 with full provenance; reusing them avoids rebuilding the HPX
  path and keeps exp59 focused on the Ray actor path.
- **Option B (reserved):** re-run the exp58 Ray-supervised HPX path **in the same allocation**
  immediately before/after the Ray actor baseline, to remove cluster-condition drift. Design the
  exp59 runner with an **optional `--hpx-recapture` hook** that shells out to the exp58 runner (read
  only; exp58 unchanged) and writes `hpx_reference_aggregate.json` from that fresh run.

**Why A first:** exp58 is the established, provenance-complete HPX path; cluster drift between exp58
(node pair medusa00/01, eno16, performance governor, HPX 1.11.0) and a near-term exp59 allocation is
expected to be small, and any comparison is already fenced to "this micro-workload, this allocation."
If the exp59 allocation differs materially (different node pair/governor/HPX build), **switch to
Option B** and record `hpx_reference_source="same_allocation_recapture"`. The aggregate must always
record `hpx_reference_source` (`exp58_curated` | `same_allocation_recapture`) and the referenced
node pair/subnet/build so comparability is auditable.

---

## 8. Correctness gates

**Ray actor baseline is valid only if:**
- Ray cluster starts cleanly (head + worker up; driver connects).
- Actors are **placed on the intended nodes** (callee `node_id`/`hostname` == nodeB; caller == nodeA).
- Calls execute on **remote nodeB** (cross-node proof: callee host/pid/node_id ≠ caller).
- All `K` measured calls return the **correct oracle** result.
- First/last call proofs pass.
- Pipeline correct counts == depths (`8/32/128`).
- **No actor/process orphan** remains (raylet/gcs/plasma/`ray::` pgrep empty on both nodes).
- **No failure/restart used.**

**HPX comparison remains valid only if (carried from exp58):**
- All exp58 islands valid; oracle matches; remote locality differs; no orphans; no shared-FS marker
  wait in Class-B; idle backoff disabled; Release / GCC-15 / HPX provenance recorded.
- The referenced exp58 aggregate has `overall="pass"` and `top_level_overwrite_guard_active=true`.

**Hard rule:** **no comparison is computed unless both sides' placement/correctness gates pass.** A
failed Ray placement gate → exp59 reports a clean skip/fail aggregate (redirected by the writer),
never a same-host-masquerading-as-cross-node number.

---

## 9. Artifact layout

Mirror the exp58 **hardened writer** policy (atomic temp+fsync+rename; phase-specific top-level
PASS names; skip/fail/local never overwrite a pass; per-run artifacts authoritative).

**Curated PASS aggregates (top level, trackable):**
- `ray_actor_same_host_aggregate.json` — **Slice 1** same-host Ray actor control row
  (`--phase same-host-control`); explicitly not cross-node, not an HPX comparison.
- `ray_actor_aggregate.json` — Ray actor path R=5 result (cross-node; Slice 4).
- `hpx_reference_aggregate.json` — either a thin **pointer record** to the exp58 aggregate
  (Option A: `{hpx_reference_source:"exp58_curated", path:"../58_.../perf_aggregate_ray_supervised.json",
  copied_summary:{...}}`) or a fresh same-allocation recapture (Option B).
- `comparison_aggregate.json` — the side-by-side band comparison (computed only when both gates pass).

**Raw / ignored artifacts:**
- `_ray_runs/<runid>/run_aggregate.json` — **authoritative** per-run record incl. raw per-call arrays.
- `_ray_runs/ray_index.jsonl` — run index.
- stdout/stderr, Ray session logs (`ray_session*/`, `*.log`).

**Writer policy fields (every top-level aggregate):** `artifact_write_policy`,
`top_level_overwrite_guard_active=true`, `top_level_aggregate_path`, `redirected_from_path`,
`overwrite_refused`. The exp59 runner should reuse the same `safe_write_aggregate` shape as exp58
(planned for Slice 1; not implemented here). Generic/un-suffixed `aggregate.json` is not used.

---

## 10. Metrics to report

**Ray actor path:**
- Per-island QD1 p50/p90/p99/min/max/mean (ns).
- Across-island median + spread (min/max of per-island summaries).
- `aggregate_mean_call_ns` vs per-call mean agreement (sanity, like exp58's ~tens-of-ns check).
- Pipeline `actions/sec` and `amortized_action_time_ns` at depths 8/32/128 (per-island + across-island).
- Startup / control-plane timings (Class-A), **reported separately**.

**Comparison (only after R=5 + gates):**
- Ray actor QD1 band vs HPX QD1 band (across-island medians + spreads, both overlaid).
- Pipeline path-characterization bands.
- **Ratios only if explicitly labeled** as "this closed-`int64` micro-workload path ratio on this
  allocation," never a broad speedup. Prefer wording like *"the Ray actor path measured
  higher/lower than the HPX action path for this micro-workload"* — and only after R=5 validates the
  direction beyond across-island jitter (apply the exp58 rule: a gap inside the jitter band is not a
  separable effect).

---

## 11. Claim hygiene

**Allowed:**
- exp59 characterizes the native Ray actor path vs the HPX action path for a **closed-`int64`
  micro-workload on Rostam**, **under disclosed measurement planes** (HPX runtime-internal C++ floor
  vs Ray Python/driver-observed floor).
- Same or comparable nodes/allocation **if achieved and recorded** (`hpx_reference_source` +
  node pair/build noted).
- Separate **startup/control-plane** and **steady-state call/action** timing.
- **Ratios only** as a *this-micro-workload path ratio*, **if** placement/correctness gates pass
  **and** the measurement-plane + batching-mechanism caveats are printed in the artifact.

**Forbidden:**
- No general Ray-vs-RayX conclusion.
- No claim that HPX is broadly faster than Ray (or vice versa).
- **No direct "same-axis latency" claim** between the HPX runtime-internal C++ action floor and the
  Ray Python-observed actor floor (different measurement planes).
- **No user-facing Python-vs-Python claim** until a Python-boundary-inclusive HPX/RayX comparator
  exists (separate future experiment).
- No production/API claim.
- No network/fabric performance claim.
- No fault-tolerance/recovery claim.
- **No comparison unless placement + correctness gates pass** on both sides.

---

## 12. Suggested slice order

- **Slice 0** — plan/schema hardening *(this document)*.
- **Slice 1** — Ray actor **local / same-host skeleton** + hardened artifact writer. Validates
  workload/timing/schema and **doubles as the same-host Ray decomposition control**
  (`same_host_ray_control`); clearly labeled non-cross-node, no comparison.
- **Slice 2** — **two-node Ray cluster under Slurm**: placement proof with **hard node affinity**
  (`NodeAffinitySchedulingStrategy(soft=False)`), cross-node proof, off-node-sample gate, clean stop,
  no orphans. **No perf claim.** See **§3a** for the review-driven hardening this slice must satisfy:
  value-encoded node proof, Ray node-id resolution gate, deterministic node-pair role assignment,
  interface/subnet parity gate, timing-free placement aggregate, `ray::` orphan-check pattern,
  driver-node honesty, and the updated Slice 2 pass gates.
- **Slice 3** — Ray actor **R=1** instrument validation (cross-node, gates, raw arrays).
- **Slice 4** — Ray actor **R=5** replicated run.
- **Slice 5** — **comparison report** against the exp58 HPX path (Option A, in-substrate C++ floor),
  with measurement-plane + batching-mechanism caveats printed; optional same-allocation HPX recapture
  (Option B) if cluster conditions differ.
- **Future** — Python-boundary-inclusive HPX/RayX endpoint comparator (user-facing-Python vs
  user-facing-Python), if desired.
- **exp60 (done)** — HPX same-node two-locality TCP control (the within-HPX L1 decomposition rung used
  by Slice 5). Whole-island failure/restart remains a separate later experiment.

---

## Roadmap (exp59 result note — final, through Slice 5)

**Experiment interpretation.** Structurally, every slice passed: Slice 2 proved hard two-node Ray
placement, Slices 3–4 produced R-banded cross-node Ray actor QD1 bands, and Slice 5 juxtaposed them
against the exp58/exp60 HPX rungs under matched node/interface parity. The measured result is two
**plane-labeled** bands — Ray Python/`ray.get`-observed (~742 µs p50 cross-node) and HPX
caller-observed C++ `async().get()` (~115.8 µs p50 inter-node) — that are **not on the same axis**. The
honest signal is the pair of **within-runtime** decompositions: ~82 % of the Ray QD1 floor and ~66 % of
the HPX QD1 floor are present **before** any network leg. This **supports** the standing interpretation
that the Ray actor path and the HPX action path bundle different layers, and **weakens** any reading of
the cross-runtime µs gap as transport- or runtime-quality evidence. What remains ambiguous: the Ray
same-host control is R=1, and `tcp_nodelay` is unverified on the HPX side. What must **not** be claimed:
any speedup, ratio, "HPX beats Ray," same-axis comparison, or production/failure/fabric claim.

**Roadmap impact: `Roadmap strengthened`.** The in-process direction now has an honest, parity-gated,
plane-labeled Ray↔HPX path characterization with symmetric within-runtime decomposition on both sides;
no direction is changed or blocked, and nothing licenses the distributed-fabric direction beyond its
existing mechanism/bootstrap evidence.

**Updated positioning (directions kept separate).**
- *In-process HPX-inside-Ray-actors:* gains a documented Ray actor cross-node baseline and the
  plane-labeled juxtaposition, with both runtimes' QD1 floors decomposed within-runtime.
- *Future distributed-fabric:* unchanged and still claim-gated (exp49–58 mechanism/bootstrap only);
  exp59/exp60 add **no** performance, fault-tolerance, multi-node, or general-fabric claim.

**One next step.** If a genuine same-axis comparison is ever wanted, build the Python-boundary-inclusive
comparator (user-facing-Python Ray vs user-facing-Python RayX/HPX) as its own scoped experiment — not by
differencing these two non-same-axis planes.
