# Internal composition / parallel registered operation note

## Status

**Status: design note; Candidate B / P1 now implemented.** This began as an
exploratory design note for the **next HPX-revealing runtime direction after
Phase 1**. Its recommended default — **Candidate B (`fanout_sum`) in its P1
(launch-all + `when_all`, queued-cancelable-only) form** — has since been
**implemented** as the first internal-composition op (one fixed native operation,
no new Python surface). This note remains the design rationale; the shipped shape is
recorded in
[rayx_runtime_phase1_summary.md](rayx_runtime_phase1_summary.md) and
[rayx_phase1_registered_operation_api.md](rayx_phase1_registered_operation_api.md)
(§7). Candidate A (`parallel_sum`) and the **P2 bounded-wave** variant remain
**unbuilt** alternatives. It lives under `docs/design/` (exploratory) deliberately
apart from `docs/reference/` (stable contracts).

It depends on, and does not relitigate, the shipped Phase 1 milestone — see
[rayx_runtime_phase1_summary.md](rayx_runtime_phase1_summary.md) for what is
already built, and
[rayx_phase1_registered_operation_api.md](rayx_phase1_registered_operation_api.md)
for the Phase-1 contract this note extends. The HPX-native lens it works within is
[rayx_runtime_hpx_design_principles.md](rayx_runtime_hpx_design_principles.md)
(P1, P3, P7, P9, P10, and the senders mechanism-currency note), and the phase map
is [rayx_runtime_problem_model.md](rayx_runtime_problem_model.md).

This note makes **no** Ray-replacement claim, **no** "HPX beats Ray" claim, and
**no** performance-superiority claim. The HPX mechanisms it weighs are weighed for
*fit* and *demonstration value*, never asserted as faster.

## Motivation

Phase 1 proved that **registered native operations run over HPX-native FIFO
`RuntimeLane`s** with value/row separation, cooperative cancellation, bounded
admission, and the `get` / `wait` / `as_completed` collection APIs. That is a real
result — but the four shipped operations (`square`, `add`, `boom`, `busy_sum`)
mostly exercise the **serving-control lifecycle** (admission, FIFO, queue/cancel,
lane occupancy), not HPX's richer **composition and parallelism** machinery.

In particular, `busy_sum` is **intentionally serial on-core lane-occupancy work**:
a single-threaded chunked accumulator whose purpose is to *hold a lane* long enough
to make queued and checkpoint-boundary cancellation deterministic. It is the
opposite of HPX's most distinctive strength — fine-grained tasks, work-stealing,
and future composition — by design.

A registered operation whose **body internally** uses HPX composition or a parallel
algorithm would *demonstrate HPX internals* (multiple tasks/cores cooperating
inside one operation) while preserving the narrow runtime contract: still one
`submit_operation` call, still one value + one row, still no new Python surface.
This note defines that direction. It is a **mechanism demonstration**, not a speed
exhibit (see Measurement cautions).

## Relationship to Phase 1

This direction is **additive within the Phase 1 contract**. It keeps, unchanged:

* `Runtime.submit_operation("name", *args)` as the only submission path.
* The **fixed native registry** (a new op is one more fixed C++ entry; **no** Python
  callable, **no** dynamic registration — D2, §7).
* `OperationResult` with **`.value` and `.row` strictly separate** (P9).
* The **exact 9-field runtime row**: `actor_id`, `submit_ns`, `start_ns`, `end_ns`,
  `total_ms`, `queue_wait_ms`, `service_ms_observed`, `status`, `error` — no new
  field, no `value` key, no harness-facade echoes.
* Cooperative **cancellation**, **bounded admission** / `QueueFullError`, and the
  `get` / `wait` / `as_completed` semantics, all as shipped.
* The `rt-hpx-` `actor_id` prefix and per-lane FIFO ordering.

**No new public Python API.** The operation is visible only as a new string id in
the registry; everything else about the Python surface is identical to Phase 1.

## What this is not

* **Not Phase 2 local stateful actors.** No lane-owned state, no registered FIFO
  methods, no actor object. (Phase 2 is deferred as the more Ray-shaped, less
  HPX-distinctive step.)
* **Not the public Phase 3 dependency-composition fork.** Phase 3 is about
  *exposing* `then` / `when_all` / `dataflow` as a (gated) **public** task-graph
  surface — the deliberate story-3 → story-2 fork. This note keeps composition
  **strictly inside one operation body**; it exposes **no** composition surface and
  does **not** take that fork. The Phase 3 public fork stays gated/deferred.
* **Not ObjectRef / object store.** The composed value is returned **directly**
  through the existing retire path (D3); no reference type, no addressable store, no
  intermediate handle crosses into Python.
* **Not arbitrary Python.** The body is pre-compiled native code; no Python callback
  runs on an HPX worker (keeps workers GIL-free — P11).
* **Not HPX actions / components / distributed locality.** Everything is **local**:
  `hpx::async` and parallel algorithms on the local runtime, never
  `HPX_PLAIN_ACTION`, components, GIDs, or localities (P2).
* **Not a performance claim.** This demonstrates *mechanism*, not speed.

## Candidate operation shapes

At least two candidates are sketched; **no candidate is chosen here** — selection is
the start of a later source slice. All use the **closed `int` type set** of Phase 1
(`OpValue = int64`) and a **deterministic, bounded** value so a smoke can assert
correctness against a closed form.

### Candidate A — `parallel_sum(n)` (parallel algorithm)

Compute `(Σ_{i=0}^{n-1} i) mod 2³¹` using an HPX **parallel algorithm**, e.g.
`hpx::transform_reduce` (or `hpx::reduce`) with `hpx::execution::par` over the range
`[0, n)`, applying the same per-step mask as `busy_sum`.

* **Deterministic value:** identical to `busy_sum(n)` — `(n*(n-1)/2) mod 2³¹` — so it
  can reuse the existing closed-form check and even be cross-checked against
  `busy_sum` for the same `n`. This makes "same value, different internal mechanism"
  a clean, testable contrast.
* **Bounded integer semantics:** per-element masking keeps the accumulator `< 2³¹`;
  the reduction must be associative under the mask (sum-mod is), so a parallel split
  yields the same result as the serial fold.
* **Cancellation checkpoint story:** **weaker than `busy_sum`.** A parallel algorithm
  does not naturally expose chunk-boundary checkpoints to the lane's
  `StopCheckpoint`. Options: (a) ship it as **queued-cancelable only**
  (`checkpoint_count == 1`), honest and simple; or (b) drive the reduction in a few
  coarse outer phases and poll `stop(...)` between phases (a hybrid that keeps *some*
  running-cancel determinism). This note does **not** decide; (a) is the smaller,
  more honest first step.
* **Coarse enough?** Only for **large `n`**. For small `n` the parallel spawn +
  reduction overhead dominates (P10) — so the *value* test can use any `n`, but any
  *mechanism observation* must use a large `n`.
* **`runtime_ops.hpp` HPX-free?** **No** — a parallel algorithm needs HPX headers
  (`hpx::execution::par`, `hpx::transform_reduce`). This **breaks the current
  HPX-free `runtime_ops.hpp` invariant** (Phase 1 deliberately kept the registry
  HPX-free; dispatch/HPX lives in `_rayx.cpp` / the lane). So `parallel_sum` would
  need its body to live **HPX-side** (in the lane/dispatch translation unit) with
  only its registry entry/metadata in `runtime_ops.hpp`, or a documented relaxation
  of the HPX-free rule for this op. This is a real structural decision for the later
  slice, flagged here.

### Candidate B — `fanout_sum(n, parts)` (future composition)

Split `[0, n)` into `parts` contiguous sub-ranges, launch the partial sums with
`hpx::async`, combine with `hpx::when_all(...).get()` plus a masked fold over the
futures, and mask the total. The **default (P1)** model launches **all** parts at
once (see the cancellation story).

* **Deterministic value:** same closed form, independent of `parts` (contiguous
  disjoint cover of `[0, n)`), so `fanout_sum(n, k)` equals `busy_sum(n)` for every
  valid `k`. `parts` becomes a second `int` arg — exercises **multi-arg typed
  dispatch** (like `add`) *and* an internal fan-out factor in one op.
* **Bounded integer semantics:** each partial stays masked; the final combine masks
  again; associativity of sum-mod makes the fan-out order irrelevant.
* **Cancellation story (default — P1, launch-all):** **queued-cancelable only.** The
  default model launches **all** `parts` partials at once with `hpx::async` and
  combines them with `hpx::when_all(...).get()` plus a masked fold, so there is **no
  honest running-cancel boundary**: by the time the body could poll `stop(...)`, every
  partial has already been launched. The op ships with `checkpoint_count == 1` — a
  **queued** cancel still skips it before service, but once it is **active** a running
  `cancel()` returns `False` (it is never armed). This is the same honesty posture as
  Candidate A option (a), stated rather than hidden: we deliberately do **not** arm
  `parts` checkpoints and then "cancel" at the combine, because that would discard
  already-completed work and claim a cancel that saved nothing — exactly the race the
  `busy_sum` count-based arming exists to avoid.
* **Optional running-cancel variant (P2 — bounded waves):** if a *real* running-cancel
  boundary is judged more valuable than the clean composition demonstration, process
  the partials in **bounded waves** of an internal `FANOUT_WAVE_SIZE` (`> 1`): launch a
  wave with `hpx::async`, combine the wave with `hpx::when_all(...).get()`, then poll
  `stop(...)` **between** waves. Then `checkpoint_count = ceil(parts / FANOUT_WAVE_SIZE)`,
  arming running-cancel only when there is more than one wave. The honesty statement is
  wave-granular: the **current wave completes**, and a cancel stops **before a later
  wave launches** (no in-flight partial is abandoned). This is the only B variant that
  earns a running-cancel claim. Cost: more code, and the running-cancel test is **more
  flake-prone** than `busy_sum`'s (whose `STRIDE` guarantees many boundaries) — it must
  be sized for several guaranteed wave boundaries and gated on `lane_stats`, never on
  sleeps. P2 is **not** the default; it is a later alternative chosen only by explicit
  decision.
* **Coarse enough?** Each partial must be coarse enough to amortize the
  `hpx::async` spawn + future (P10); tiny `n/parts` measures the boundary, not the
  work. `parts` must be validated (`>= 1`, and bounded to avoid spawning absurd
  counts).
* **Default thread count caveat (important):** at the default `hpx_threads=1` the op
  exercises **composition wiring and cooperative scheduling**, not **parallel execution
  across cores**. HPX's default scheduling is cooperative/non-preemptive, so a
  CPU-bound partial with no internal suspension point runs to completion before the
  next: with a single worker the partials execute **serially / back-to-back on one
  worker**, never simultaneously — **no cross-core parallelism happens at default
  `hpx_threads=1`**. Real cross-core parallelism needs **multiple HPX worker threads**
  *and* coarse enough per-part work (P10). The value is thread-count-independent (value
  tests may run at the default); any *parallel-execution* observation must raise
  `hpx_threads`. **No performance claim** either way.
* **`runtime_ops.hpp` HPX-free?** **No** — `hpx::async` / `when_all` are HPX types, so
  the body cannot stay in the HPX-free header. Same structural consequence as A: the
  composed body lives HPX-side. **Preferred resolution (two-registry merge):**
  `runtime_ops.hpp` **remains HPX-free** and keeps only pure helpers/constants (e.g.
  `FANOUT_PARTS_MAX` and `fanout_sum_checkpoints`); the `fanout_sum` `OpEntry` — its
  arity-table entry, the HPX body, and the HPX registry (`hpx_registry()` /
  `hpx_op_arities()`) — lives in a new HPX-side header `runtime_ops_hpx.hpp` (which
  includes `<hpx/hpx.hpp>`). `_rayx.cpp` then **merges** the HPX-free arity table
  (`op_arities()`) with `hpx_op_arities()` for the Python-boundary table, and checks
  both registries at dispatch. Relaxing the HPX-free rule is the documented fallback.
  Do **not** silently leak HPX types into the
  HPX-free header.

### Optional — tree-reduction shape

A `tree_sum(n)` doing a balanced binary `when_all` reduction is mentioned only as a
variant of B (a different composition topology). It adds combinator depth but no new
contract concern over B; not recommended as the first op (more code, same value,
same cautions). Listed for completeness.

**Takeaway:** B (`fanout_sum`) is the cleaner *composition* demonstrator (multi-arg
dispatch, and future composition — the P3-internal mechanism — used privately); A
(`parallel_sum`) is the cleaner *parallel-algorithm* demonstrator. In its **default
(P1) form B is queued-cancelable only** and has **no** running-cancel advantage over A;
a running-cancel boundary exists **only** in the optional **P2 bounded-wave** variant.
Both break the HPX-free `runtime_ops.hpp` invariant and so force a "where does the op
body live" decision — resolved by housing the body in a new HPX-side
`runtime_ops_hpx.hpp` and **merging two registries**, keeping `runtime_ops.hpp`
HPX-free. The chosen **default direction** for the first composed op is **B in its P1
form**; P2 stays a later alternative chosen only if running-cancel boundaries are
explicitly prioritized over the clean composition demonstration. (The op selection
itself remains pending a separate source slice + approval.)

## HPX mechanisms to consider

* **`hpx::future`** — the result channel for each internal task; the same
  move-once/`.get()` semantics the lane already uses, kept **internal** (never
  surfaced to Python).
* **`hpx::async`** — spawns each internal partial (Candidate B). On an HPX worker its
  `.get()` is a cooperative suspension, so the lane worker yields rather than blocks
  (consistent with §11b).
* **`when_all` / `then`** — combine the internal futures (Candidate B / tree). This
  is exactly the composition the problem model reserves for **dependencies** (P3) —
  used here **privately inside one op**, not exposed.
* **Parallel algorithms (`hpx::transform_reduce` / `hpx::reduce`) with
  `hpx::execution::par`** — Candidate A; the most HPX-distinctive (work-stealing
  across the pool), but the least checkpoint-friendly for cancellation.
* **Sender/receiver expression (future axis only).** Per the principles
  mechanism-currency note, a later revision *could* express the fan-out with
  `hpx::execution::experimental` senders (`schedule` / `when_all` / `let_value`).
  This is recorded as a **direction**, not a plan; the first implementation should
  use the **stable futures/`hpx::async` path**, not be rewritten around senders.
* **Executor / pool placement constraints from HPX bootstrap.** Internal tasks run on
  whatever pool the dispatch executor names. Per §11c, **distinct `control` / `work`
  pools are configured at HPX initialization**, not per `Runtime()`, and the runtime
  **shares the one process HPX bootstrap** — so internal parallelism shares cores
  with lane-control work **unless** the shared bootstrap declares separate pools.
  Internal fan-out does **not** create isolation on its own; this is a placement
  constraint to state honestly, not a perf lever to pull.

## API and result model

* The operation is still invoked as `submit_operation("parallel_sum", n)` /
  `submit_operation("fanout_sum", n, parts)` — **one submission, one
  `RuntimeFuture`**.
* It returns **one `OperationResult`**: one `.value` (the masked total) and **one**
  9-field `.row`. Internal parallelism is invisible to the result shape.
* It emits **no child rows** and **no per-internal-task records.** The internal
  futures/tasks are an implementation detail; they never become rows, never enter
  JSONL (the runtime emits none), and never touch the v1 analyzer (D4, P9).
* It **does not expose internal futures to Python.** No `RuntimeFuture` is created for
  any internal task; Python sees only the outer operation's handle.
* `row["service_ms_observed"]` measures the **outer operation's lane-occupancy
  lifecycle** (work-shape-agnostic, exactly as in §12) — i.e. how long the lane was
  occupied servicing this operation — **not** the sum or timeline of every internal
  task. Same field, same semantics, different internal work shape.

## Testing strategy

Mirrors the Phase-1 runtime smoke / unit / integration split; **no timing or
performance assertion** anywhere.

* **Deterministic value tests:** `parallel_sum(n)` / `fanout_sum(n, parts)` equal the
  closed form `(n*(n-1)/2) mod 2³¹`; cross-check that they equal `busy_sum(n)` for the
  same `n` (and, for B, across several `parts`). Edge cases: `n = 0`, `n = 1`,
  `parts = 1`, `parts = n`.
* **Boundary validation:** arity/type rejected at the Python boundary (e.g.
  `fanout_sum` needs two `int`s; `parts >= 1`; `n >= 0`) — `TypeError` / `ValueError`
  before the crossing, before any `RuntimeFuture` (consistent with Phase 1 §7).
* **Cancellation tests:** for the **default B/P1** (launch-all, queued-cancelable
  only), assert exactly that posture — a **queued** cancel (behind a long op, as in
  Phase 1) retires `status == "cancelled"` with `.row` readable and `.value` raising
  `OperationCancelledError`, while an **active** running `cancel()` returns `False` and
  the op **completes normally** (no faked checkpoint). This is the same posture as
  Candidate A option (a). A **fan-out-boundary running cancel** (retires
  `status == "cancelled"`, `.row` readable, `.value` raises `OperationCancelledError`)
  is tested **only** for the optional **P2 bounded-wave** variant, where a real
  between-wave boundary exists.
* **Row-shape tests:** exact 9-field key set; `"value" not in row`; no
  `label`/`chunks`/`chunk_delay_ms`/`chunks_completed`; `actor_id` starts `rt-hpx-`.
* **`get` / `wait` / `as_completed` compatibility:** a batch including the new op
  behaves like any other (input-order collect, non-consuming wait, yielded handles).
* **Harness untouched:** `bench/smoke_rayx.py` and the v1 schema golden unchanged;
  `rayx.__all__` unchanged.

## Measurement cautions

* **Avoid "HPX faster" claims.** This direction demonstrates that an operation can
  *compose internal HPX work*; it is **not** evidence that parallel/fan-out is faster
  than serial `busy_sum`, nor that HPX beats anything.
* **Tiny `n` is meaningless for mechanism.** For small `n` (and small `n/parts`),
  spawn + future + reduction overhead dominates the actual arithmetic (P10) — a small
  case measures the **boundary**, not the work. Value tests may use any `n`; any
  mechanism *observation* must use coarse `n`.
* **Default `hpx_threads=1` ≠ parallel.** With one HPX worker the partials interleave
  cooperatively; they do **not** run across cores. The smoke / example / CI run at the
  default and so exercise composition *wiring*, not parallel execution. Any "fanned out
  across cores" statement requires `hpx_threads > 1` **and** coarse `n/parts` — and even
  then reports mechanism, never speed.
* **If ever measured, report mechanism observations only**, with **explicit overhead
  accounting** that separates the Python→C++ crossing, lane dispatch, and
  spawn/compose cost from the arithmetic — never a bare wall-time comparison.
* **Separate scheduling mechanism from workload speed.** "The op internally fanned out
  across the pool" is a *mechanism* statement; "it was faster" is a *performance*
  statement and is out of scope. Keep them apart, as the corpus does for `std` vs
  `hpx` lanes.

## Risks and guardrails

* **Registry-as-arbitrary-execution backdoor (D2 / §15).** A composed op is still a
  **fixed native entry**; nothing here admits a Python callable or dynamic
  registration. The standing risk is unchanged and the mitigation is unchanged.
* **HPX-free `runtime_ops.hpp` invariant pressure.** Both candidates need HPX headers
  in the body, which the registry header currently avoids. The later slice must
  decide: keep `runtime_ops.hpp` HPX-free by housing the composed body **HPX-side**
  (registry entry/metadata only in the header), or relax the invariant for this op
  with a documented reason. **Do not** silently leak HPX types into the HPX-free
  header.
* **Cancellation honesty.** If a candidate cannot offer running cancel (Candidate A),
  ship it **queued-cancelable only** and say so — do not fabricate a checkpoint count
  that claims a boundary the algorithm does not actually honor (the `busy_sum`
  count-based arming exists precisely to avoid that race).
* **No public composition surface.** Internal `when_all` / `then` must not become a
  Python-visible task-graph API — that is the gated Phase 3 fork, out of scope here.
* **Placement honesty.** Internal parallelism shares cores with lane-control work
  unless the shared HPX bootstrap declares distinct pools; do not imply isolation.
* **Schema/analyzer frozen.** No new row field, no child rows, no JSONL, no analyzer
  change (D4, P9).
* **No performance claim** anywhere in the op, its tests, or its write-up.

## Implementation slice (shipped)

This was the docs-first step; the slice that followed added **exactly one**
operation, **Candidate B (`fanout_sum`)** in its **P1 (launch-all,
queued-cancelable-only)** form. **Candidate A (`parallel_sum`)** and the **P2
bounded-wave** variant remain unbuilt alternatives — one op, not both. What the slice
did, as planned:

* Applied the HPX-free resolution: the composed body + entry live in the new HPX-side
  `runtime_ops_hpx.hpp` (`hpx_registry()` / `hpx_op_arities()`); `runtime_ops.hpp`
  stays HPX-free with only pure helpers (`FANOUT_PARTS_MAX`, `masked_range_sum`,
  `fanout_sum_checkpoints`); `_rayx.cpp` merges `op_arities()` with `hpx_op_arities()`
  and checks both registries at dispatch.
* Added `fanout_sum` coverage to `bench/smoke_rayx_runtime.py` (value + parts-
  independence incl. `parts > n` + 9-field row + boundary validation + queued-only
  cancel invariant + admission), unit coverage for the new boundary validation
  (`tests/unit/test_validate.py`), and integration coverage for the contract
  (`tests/integration/test_runtime_contract.py`).
* Touched **no** schema/analyzer/harness code and added **no** new Python public
  surface (only the fixed operation name).
* Makes **no** performance claim and reuses the closed-form value check (and the
  `busy_sum` cross-check) so correctness is deterministic.

**Where this sits in the roadmap.** `fanout_sum` is the *composition extension* of
the Phase 1 runtime, not the next serious axis. Per the problem model's "Current
strategic target (post-Phase-1)", the next serious design axis is the **closed
value/type model** (escaping `int64`-only), then local stateful native actors and a
typed native plugin ABI; more *internal* composition (and the gated public
composition fork) is a later, lower-marginal-value direction. Distribution stays
design-only and much later. See
[rayx_runtime_problem_model.md](rayx_runtime_problem_model.md).

## See also

* [rayx_runtime_phase1_summary.md](rayx_runtime_phase1_summary.md) — the shipped
  Phase 1 milestone this direction extends.
* [rayx_phase1_registered_operation_api.md](rayx_phase1_registered_operation_api.md)
  — the Phase 1 contract (registry, 9-field row, `busy_sum`/`STRIDE`, the HPX-free
  `runtime_ops.hpp` rule, lane-bound `StopCheckpoint`, §11c mechanism non-choices).
* [rayx_runtime_hpx_design_principles.md](rayx_runtime_hpx_design_principles.md) —
  P1 (registry + `hpx::async`), P3 (future composition, here used privately), P7
  (cooperative cancellation), P9 (rows separate from values), P10 (operation
  granularity), P11 (GIL-free workers), and the senders mechanism-currency note.
* [rayx_runtime_problem_model.md](rayx_runtime_problem_model.md) — the phase map;
  this note is **not** Phase 2 (local actors) and **not** the public Phase 3 fork.
