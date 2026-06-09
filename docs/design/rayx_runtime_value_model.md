# rayx.runtime value/type model design note

## Status

**Status: design / modeling only.** This is an exploratory design note for the
**next serious design axis** of the experimental `rayx.runtime` prototype — a closed
typed value model — not implementation, not a delivery commitment, and not a shipped
API. It lives under `docs/design/` (exploratory) deliberately apart from
`docs/reference/` (stable contracts). **Nothing here is built.**

It depends on, and does not relitigate, the shipped milestones: the Phase 1
registered-operation runtime (see
[rayx_runtime_phase1_summary.md](rayx_runtime_phase1_summary.md) and
[rayx_phase1_registered_operation_api.md](rayx_phase1_registered_operation_api.md))
and the `fanout_sum` internal-composition extension
([rayx_runtime_internal_composition_note.md](rayx_runtime_internal_composition_note.md)).
It is placed in the roadmap by the strategic-target note in
[rayx_runtime_problem_model.md](rayx_runtime_problem_model.md) ("Current strategic
target (post-Phase-1)") and works within the HPX-native lens of
[rayx_runtime_hpx_design_principles.md](rayx_runtime_hpx_design_principles.md)
(P6 — closed typed arg/result set; P9 — rows separate from values; P11 — GIL-free
workers).

It makes **no** Ray-replacement claim, **no** "HPX beats Ray" claim, and **no**
performance-superiority claim.

### Implementation status

The **first substrate slice (V1)** of this model has now **shipped experimentally**:
typed-signature registry metadata (`{op_id: {arg_types, result_type}}`, **int64
only**), typed Python-boundary validation driven by that table, and an explicit
`[INT64_MIN, INT64_MAX]` range check. The four core ops plus `fanout_sum` are
re-expressed with explicit `int64` signatures; their behavior is **unchanged** except
that an `int` outside the `int64` range is now rejected at the Python boundary with
`ValueError` (before any future), instead of failing opaquely at the pybind crossing.
The **second slice (V3, combined)** has now **also shipped experimentally**. A
standalone "V2" (a one-alternative `std::variant<int64_t>` with no behavior change)
was **deliberately folded into V3** rather than shipped on its own: a single-alternative
variant is speculative scaffolding (the `vector<int64>` crossing wrap and the dead
wrong-tag path get reshaped anyway), so the variant arrives **together with its first
real second alternative, `double`**, and one fixed `double`-consuming op. V3 ships:

* an **internal value channel** `OpValue = std::variant<std::int64_t, double>`,
  `OpArgs = std::vector<OpValue>` — internal **only**, never serialized or exposed as a
  `std::variant` across pybind or any ABI;
* **typed Python→C++ marshalling**: the public boundary stays
  `rayx.runtime._validate` (per-arg types, int64 range, **strict-finite `double`**),
  and `_RuntimeEngine.submit_operation` accepts a Python sequence and marshals it into
  `OpArgs` C++-side (`bool` rejected before `int`; `int → int64`; `float → double`);
* one new op **`scale_double(double x, double factor) -> double`** = `x * factor`,
  instantaneous (`one_checkpoint`), deterministic for exactly-representable doubles,
  with **no internal HPX fan-out** (float sum is not associative) and no performance
  intent;
* **result conversion** by an explicit `std::visit` in `RuntimeFuture.result()`
  (`int64 → int`, `double → float`) on the **Python thread, GIL held** — never on an
  HPX worker;
* **`bytes` remains deferred** (gated hardest); no `ObjectRef`/object store, no pickle,
  no arbitrary Python, no numpy/Arrow, **no schema/analyzer change** (the row is the
  same exact 9 fields, still no `value`/type field), and **no performance claim**.

The existing `int64` ops (`square`/`add`/`boom`/`busy_sum`/`fanout_sum`) keep **exact
public behavior**; the only V3-era behavior addition is the new `double` op and its
strict-`float` boundary rules.

**Checkpoint-count safety (corrected).** An **op-body** wrong-tag extraction
(`as_int64`/`as_double` inside the op) maps to a `status="failed"` row through
`make_op_task`'s catch. But `checkpoint_count` runs **before** the task/future exist,
so it cannot rely on failed-row mapping. The checkpointed ops (`busy_sum`,
`fanout_sum`) therefore read their arg **defensively** (valid tag → real count, wrong
tag → `1`), and the op body then produces the failed row — so even the raw bypass
yields a clean failed row rather than an exception raised at submit. The public path is
fully protected by Python validation upstream.

Framing for V1/V3 and the later slices:

* **Neither V1 nor V3 is an HPX mechanism step.** Both are Python-boundary /
  type-substrate steps. The change to `RuntimeResult.value`'s type is transparent to
  the HPX machinery: `hpx::future<RuntimeResult>` / `hpx::promise<RuntimeResult>` are
  instantiated on the *struct type* (unchanged), and the lane, the cancellation token,
  `hpx::wait_some`, and the `fanout_sum` `hpx::async`/`when_all` mechanics only
  default-construct/move `RuntimeResult` — they never read `.value`. So they are
  **unchanged** through V3 (the variant is default-constructible/movable).
* The **HPX-relevant invariant is preserved**: native operation bodies remain
  **GIL-free** (they produce a native `OpValue`), and native→Python value conversion
  stays on the **Python thread** in `RuntimeFuture.result()` (P11) — never on an HPX
  worker.
* For V2/V3, keep `std::variant` an **internal** representation only: do **not** expose
  its (implementation-defined) layout across any plugin ABI. The ABI form is a
  fixed-layout tagged union; the distributed-someday form relies on
  `hpx::serialization`.
* **`bytes` stays gated hardest**: a heap-backed payload is the first real
  `hpx::future` payload cost (the first non-trivially-relocatable `OpValue`
  alternative) and the primary object-store-drift risk. Ship it only on a concrete
  bounded-typed-value need, never as data movement.
* A future **numeric** HPX op should **not** copy `fanout_sum`'s manual
  async-per-part fan-out as if it were best practice. `fanout_sum` is a composition
  *demonstration*; for real parallel numeric work prefer HPX parallel algorithms
  (e.g. `hpx::transform_reduce` with `hpx::execution::par` + an executor) over a
  hand-rolled `hpx::async`-per-part gather.

## Motivation

Phase 1 proved that **fixed registered native operations run on HPX-native FIFO
lanes** with value/row separation, cooperative cancellation, bounded admission, and
the `get` / `wait` / `as_completed` collection APIs; `fanout_sum` proved that an
operation body can use **internal HPX composition** (`hpx::async` + `when_all`)
without changing the Python surface. Both are real results.

But the runtime is still limited by a **single value shape: `int64`**. Concretely,
in the shipped code:

* arguments cross the boundary as `std::vector<std::int64_t>` (the Python facade
  validates them to a list of `int` via `validate_call` and passes them to
  `_RuntimeEngine.submit_operation(op_id, args)`);
* the result value is `OpValue = std::int64_t` (`runtime_ops.hpp`), surfaced to
  Python as an `int`;
* the registry metadata is **arity-only** (`op_arities()` / `hpx_op_arities()` map
  `op_id -> int`).

So every operation — `square`, `add`, `boom`, `busy_sum`, `fanout_sum` — is an
`int64`-in / `int64`-out function by construction. That is the **binding constraint
on everything real**: a runtime that can only pass and return one integer type
cannot express most useful native work, cannot grow a meaningful operation set, and
cannot underpin a later typed plugin ABI or local stateful actors.

The next serious unlock is therefore **not more toy operations and not distribution**
— it is a **closed, typed value model**: a small, explicit set of argument/result
types, validated at the Python boundary, represented natively without the GIL, and
returned through the existing value/row split unchanged. This note designs that
model; it deliberately does **not** add an operation that uses it (that is a later,
separately-approved slice).

## Non-goals

This model is **closed and narrow by design**. It is explicitly **not**, at any
point in this axis:

* an `ObjectRef` or any reference/handle type (D3; the value is returned directly);
* an object store (the most-excluded subsystem; HPX's weakest ground);
* arbitrary Python objects, `pickle`, or any serialized-callable path (D2 / P11);
* dynamic Python callables or dynamic Python registration (the registry stays
  fixed/native);
* numpy / Arrow / zero-copy / shared-memory buffers (no data plane);
* nested containers (lists/dicts/tuples), user-defined classes, or a general
  serialization format;
* a change to the frozen v1 benchmark JSONL schema or the analyzer (D4 / P9);
* a performance claim or a microbenchmark of conversion cost.

The model is a way to pass and return a **few well-defined scalar/bounded values**,
not a way to move data. The moment "bytes" or "the type set" starts being used as a
payload-transport or object-store substitute, the model has failed its purpose (see
Risks).

## Proposed initial type set

A minimal closed set, ordered by safety. `int64` already exists; `double` and
`bounded bytes` are the additive candidates.

### `int64` (already shipped — formalized here)

* **Python accepted values:** `int` only; `bool` rejected explicitly (it is an `int`
  subclass), as today in `validate_call`.
* **C++ representation:** `std::int64_t` (`OpValue` today).
* **Validation:** value must fit `[INT64_MIN, INT64_MAX]`. Python `int` is
  arbitrary-precision, so an explicit **range check at the boundary** is required
  (`ValueError`/`OverflowError`) rather than relying on an implicit pybind cast
  failure; this makes the rejection deterministic and well-messaged.
* **Result → Python:** Python `int`.
* **Bounds:** the 64-bit range; per-op domain rules (e.g. `n >= 0`) stay separate,
  as they are now.
* **Risks:** none new; this only formalizes current behavior.

### `double` (candidate; smallest additive delta)

* **Python accepted values:** `float` (and, by explicit decision, **not** `int`
  implicitly — require an explicit `float`, mirroring the strict `bool`-as-`int`
  rejection, to avoid silent widening). `bool` rejected.
* **C++ representation:** `double`.
* **Validation:** **reject `NaN` and `±inf` at the boundary** (`ValueError`), mirroring
  the existing `validate_timeout` discipline, so arguments stay finite and
  deterministic. (Whether a *result* may be non-finite is an open question — see
  Result/value model; the conservative default is finite-only both ways.)
* **Result → Python:** Python `float`.
* **Bounds:** IEEE-754 double; finite-only under the default rule above.
* **Risks:** float equality / determinism in tests — value tests must compare against
  a closed-form expectation with an exact or explicitly-toleranced check; associativity
  under floating point is **not** exact, so a future `double` fan-out op could not
  reuse the integer sum-mod associativity argument verbatim (a caution for whoever
  adds such an op later — not this slice).

### `bounded bytes` (candidate; highest-risk — gate hardest)

* **Python accepted values:** `bytes` only (**no** implicit `str` → `bytes`; require
  an explicit `bytes`, see Python boundary validation). `bytearray` is an open
  question — `bytes` only to start.
* **C++ representation:** by-value copy into `std::string` or `std::vector<std::byte>`
  (one chosen, not both). By value, never a borrowed view into a Python buffer.
* **Validation:** a hard **size cap** (a small constant, e.g. on the order of a few
  KiB to at most ~1 MiB — chosen deliberately *small*), enforced at the boundary;
  over-cap raises `ValueError`. The cap exists to keep `bytes` a **bounded typed
  argument**, not a data-transport channel.
* **Result → Python:** Python `bytes` (a fresh copy), constructed **on the Python
  thread at `result()` time** (GIL held there), never on an HPX worker (P11).
* **Bounds:** the size cap, both for arguments and results.
* **Risks:** this is where the model is most likely to **drift into an object store /
  payload transport**. It must be justified by a concrete need for a small opaque
  typed value (e.g. a fixed-size key or checksum), bounded hard, and never described
  as "data movement." If no such need is demonstrated, **`bytes` should not ship** —
  `int64` + `double` may be the whole set.

### Explicitly excluded (and why)

| Excluded | Why excluded now |
|---|---|
| Arbitrary Python object | Re-imports pickle + GIL + arbitrary execution; breaks D2/P11. |
| `pickle` | A serialized-closure / object-store backdoor; not a typed value. |
| `ObjectRef` | Reference/handle semantics; demoted + evidence-gated (D3), not a value type. |
| Object store | HPX's weakest area; the most-excluded subsystem. |
| numpy / Arrow / zero-copy | A data plane; out of scope; would need buffer-lifetime + zero-copy design. |
| nested lists / dicts / tuples | A general serialization format; type-creep toward "arbitrary object". |
| user-defined classes | Arbitrary Python objects by another name. |

## C++ representation options

The representation must carry a **type tag + value** for each argument and for the
result. Options:

1. **`std::variant<std::int64_t, double, Bytes>`** (where `Bytes` is `std::string` or
   `std::vector<std::byte>`). Natural C++ choice; type-safe; `std::visit` / `std::get`
   to extract. **Cost:** every op body must extract its typed args (`std::get<...>`)
   instead of indexing `a[i]` as a raw `int64`, adding boilerplate to the existing
   ops; and the variant's alternative order becomes a *de facto* tag enumeration.
2. **A tagged struct `{ Tag tag; int64_t i; double d; Bytes b; }`** (or tag + union).
   More explicit control over the tag enum (useful for a future ABI), but manual and
   error-prone vs `std::variant`.
3. **Typed `OpValue` + typed args vector.** `using OpValue = std::variant<...>;` and
   `using OpArgs = std::vector<OpValue>;`. The `OpFn` signature changes from
   `OpOutcome(const std::vector<std::int64_t>&, const StopCheckpoint&)` to take
   `const OpArgs&`; `RuntimeResult.value` changes from `std::int64_t` to `OpValue`.
4. **Typed result.** `OpOutcome.value` / `RuntimeResult.value` become the variant;
   `has_value` / `status` / `error` stay exactly as they are.

**Error handling for unsupported types** stays a **boundary** responsibility (Python
rejects before crossing); the native side keeps a **defensive** check (a wrong-tag
`std::get` would otherwise throw `std::bad_variant_access`, which `make_op_task`
already maps to a `status="failed"` row — consistent with the `fanout_sum` defensive
guard) so the private/native bypass cannot crash.

**Recommendation (for the later slice, not decided here):** option 1/3 — a
`std::variant`-based `OpValue` and `OpArgs` — as the smallest type-safe step, keeping
`OpOutcome` / `RuntimeResult` field names unchanged except `value`'s type.

**ABI / plugin implications (noted, not designed):** whatever representation is
chosen becomes the surface a future **typed native plugin ABI** would build on — the
**tag enumeration and by-value semantics would become ABI-stable commitments**. So
the value model should pick a representation whose tag set and value semantics can be
**versioned and extended without churn** (append-only tags, by-value copies, no
exposed `std::variant` layout across an ABI boundary). **This note does not design the
plugin ABI**; it only flags that the value model is its foundation and should not
paint it into a corner.

## Python boundary validation

All type validation happens at the Python boundary (in `runtime/_validate.py`,
extending `validate_call`), **before** the single Python→C++ crossing and **before**
any `RuntimeFuture` is created — exactly as today. The registry table passed to
`validate_call` would carry **per-argument types** (see Operation registry
implications), not just arity. Rules:

* **`bool` rejected** wherever an `int`/`float` is expected (it is an `int` subclass);
  explicit, as today.
* **`int64` arg:** must be a Python `int`, within `[INT64_MIN, INT64_MAX]` →
  out-of-range raises `ValueError`/`OverflowError`.
* **`double` arg:** must be a Python `float`; **no implicit `int`→`float`** widening
  (require an explicit `float`); `NaN`/`±inf` rejected → `ValueError`.
* **`bytes` arg:** must be a Python `bytes`; size `<=` the cap → over-cap raises
  `ValueError`; **no implicit `str`→`bytes`** (a `str` is a `TypeError`) unless a
  concrete need with an explicit encoding is later justified — implicit text encoding
  is a silent-correctness hazard and is excluded by default.
* **Unsupported types** (anything outside the declared arg type) raise `TypeError`.
* **Invalid bounds** (range / size / non-finite) raise `ValueError`.
* **No future is created on any boundary failure** — validation precedes
  `submit_operation`, so a rejected call has zero runtime side effects (mirrors the
  current `validate_call` discipline and the `fanout_sum` boundary tests).

The per-arg type declaration also lets the boundary report **which argument** failed
and **what type was expected**, improving on the current arity-only messages.

## Result/value model

**Unchanged — the value model touches only the value channel, never the row.**

* `OperationResult.value` returns the typed value (Python `int` / `float` / `bytes`)
  for a completed operation; **idempotently re-readable** (not consume-once).
* `OperationResult.row` stays the **exact 9-field row** (`actor_id`, `submit_ns`,
  `start_ns`, `end_ns`, `total_ms`, `queue_wait_ms`, `service_ms_observed`, `status`,
  `error`) — **no `value` key**, no type field, no new field. The value's *type* is
  carried by the Python object itself, not by the row.
* **Failed / cancelled rows remain inspectable**; `.value` still raises
  `OperationFailedError` / `OperationCancelledError`. The value's type is irrelevant
  to a failed/cancelled outcome (`has_value == false`).
* **Conversion placement (P11):** native→Python conversion (int64→`int`,
  double→`float`, bytes→`bytes` copy) happens **on the Python thread inside
  `RuntimeFuture.result()`** with the GIL held — **never on an HPX worker**. Operation
  bodies remain pure native code producing a native `OpValue`; no Python object is
  ever constructed on a worker thread.

**Open question (flag, do not decide here):** whether a *result* may be non-finite
`double` (`NaN`/`inf`) even though *arguments* may not. The conservative default is
finite-only both ways; a future op that legitimately produces non-finite results
would have to justify relaxing it.

## Operation registry implications

The registry metadata must evolve from **arity-only** to a **typed signature**:

```text
op_id  ->  (arg_types: [Type...], result_type: Type, constraints...)
```

Concretely (design sketch, not implementation):

* `OpEntry` would gain an **argument type list** and a **result type** alongside its
  `fn` and `checkpoint_count`. The existing `arity` becomes `len(arg_types)`.
* `runtime_op_table()` (today returns `{op_id: arity}`) would expose enough for the
  Python boundary to validate types — e.g. `{op_id: {"arg_types": [...],
  "result_type": ...}}` — while keeping the **HPX-free `runtime_ops.hpp` / HPX-side
  `runtime_ops_hpx.hpp` split** and the **two-registry merge** unchanged in shape.
* Per-op **domain constraints** (e.g. `busy_sum`'s `n >= 0`, `fanout_sum`'s
  `1 <= parts <= 1024`) stay as today — a separate, per-op check layered on top of the
  generic type validation.
* The four existing ops would be **re-expressed with explicit `int64` signatures**;
  their behavior is identical (this is the substrate, not a behavior change).

This keeps validation generic and table-driven (no per-op Python code), exactly as
`validate_call` is today — only the table grows from arity to typed signatures. **This
is design, not implementation.**

## Testing strategy

Mirrors the Phase-1 / `fanout_sum` unit + integration + smoke split; **no timing or
performance assertion** anywhere.

* **Valid args/results per type:** round-trip `int64`, `double`, and (if it ships)
  `bytes` through a typed operation; the value comes back as the correct Python type
  and value. (Until a typed op is added, the substrate is proven by re-expressing the
  existing `int64` ops and asserting identical behavior.)
* **Unsupported type rejection:** a wrong Python type for a declared arg raises
  `TypeError` at the boundary, before any future.
* **`bytes` cap:** at-cap accepted, over-cap raises `ValueError`; no future created.
* **`int` range:** in-range accepted, out-of-`int64`-range raises
  `ValueError`/`OverflowError`.
* **`float` NaN/inf:** `NaN`/`±inf` arguments raise `ValueError`; finite accepted.
* **`bool` rejection:** `bool` rejected wherever `int`/`float` is expected.
* **Row shape unchanged:** the exact 9-field key set; `"value" not in row`; no
  type field; `actor_id` starts `rt-hpx-`.
* **`get` / `wait` / `as_completed` compatibility:** a batch including typed ops
  behaves like any other (input-order collect, non-consuming wait, yielded handles).
* **Failed / cancelled behavior unchanged:** typed ops still produce inspectable
  failed/cancelled rows; `.value` raises the typed errors.
* **No schema/analyzer changes:** the v1 golden and `bench/smoke_rayx.py` stay green;
  `rayx.__all__` unchanged.
* **Defensive native bypass:** a wrong-tag value reaching the body via the private
  path maps to a `status="failed"` row (not a crash), like the `fanout_sum` guard.

The **pure unit layer** (`tests/unit/`) must stay import-light: type-validation logic
should be testable against a representative typed table **without** `_rayx` (extending
the current `_validate.py`-by-file-path pattern).

## Roadmap placement

Per [rayx_runtime_problem_model.md](rayx_runtime_problem_model.md) ("Current strategic
target (post-Phase-1)"):

```text
done:                    Phase 1 registered native runtime (square/add/boom/busy_sum)
done:                    fanout_sum internal-composition extension
next (this note):        closed value/type model
later:                   local stateful native actors
later:                   typed native plugin ABI
much later / design-only: distributed HPX localities/actions/components
possibly never:          ObjectRef / object store / Ray-compatible clone
```

The value/type model is the **prerequisite substrate** for the two "later" axes: a
typed plugin ABI needs a stable typed value representation, and useful stateful actors
need to pass/return more than one integer type. Distribution stays design-only and
much later; `ObjectRef`/object store stay evidence-gated and possibly never.

## Risks and guardrails

* **Type creep toward arbitrary Python.** Each added type widens the boundary; the set
  must stay **small, closed, and explicitly enumerated**. New types are a deliberate
  decision with a concrete need, never a convenience. Containers / objects / pickle
  stay excluded.
* **Accidentally creating an object store.** Returning/holding values is fine; making
  them **addressable, reusable, or referenceable** is the `ObjectRef`/store line (D3) —
  not crossed by a value type.
* **`bytes` becoming unbounded payload transport.** The hard size cap is load-bearing.
  If `bytes` cannot be justified as a *small bounded typed value*, it should not ship;
  `int64` + `double` may be the whole set. Never describe `bytes` as data movement.
* **C++ `std::variant` complexity.** Every op body gains extraction boilerplate and a
  defensive wrong-tag path; keep the type set small so the variant stays small and the
  `std::visit`/`std::get` surface stays manageable.
* **ABI-stability pressure.** The tag enumeration and value semantics become the
  foundation a future plugin ABI commits to; choose an append-only, by-value,
  layout-private representation so the value model does not force ABI churn later.
  (The ABI itself is **not** designed here.)
* **Python conversion reintroducing GIL work in the wrong place.** All native→Python
  conversion must stay on the Python thread in `result()` (GIL held there); **never**
  construct a Python object on an HPX worker (P11). A `bytes` copy on a worker would be
  a correctness/architecture regression.
* **Performance claims from type microbenchmarks.** Conversion/validation cost is
  boundary overhead (P10); it must **not** be presented as a performance result, and no
  type microbenchmark is in scope.

## Smallest possible implementation slice

* **This note is the docs-first step** (no source).
* A **later source slice** (separate, plan + approval per the working rules) would add
  the **typed value substrate only**, *without adding any new operation*:
  * introduce a typed `OpValue` / `OpArgs` (`std::variant`-based) and change the
    `OpFn` / `RuntimeResult.value` types accordingly, keeping the HPX-free
    `runtime_ops.hpp` / HPX-side `runtime_ops_hpx.hpp` split and the two-registry
    merge unchanged in shape;
  * evolve the registry metadata from arity-only to **typed signatures**, and
    re-express the four existing ops (`square`/`add`/`boom`/`busy_sum`) plus
    `fanout_sum` with explicit `int64` signatures — **identical behavior**;
  * extend `runtime/_validate.py` with per-type boundary validation (int64 range,
    strict `float`, finite-only, `bytes` cap, strict `bool`/`str` rejection) driven by
    the typed table;
  * add unit/integration/smoke coverage proving the substrate (existing ops unchanged;
    type rejections at the boundary; row shape and collection/cancel behavior
    unchanged; v1 golden + harness smoke green);
  * touch **no** schema/analyzer/harness code and add **no** new Python public
    surface.
* A **separate, later** slice would add the **first operation that actually consumes a
  new type** (e.g. a `double` or `bounded bytes` op) — that is an *operation-adding*
  step, explicitly **out of scope for the substrate slice** and gated on its own plan.
* Throughout: **no** `ObjectRef`/object store, **no** arbitrary Python/pickle, **no**
  schema/analyzer change, and **no** performance claim.

## See also

* [rayx_runtime_problem_model.md](rayx_runtime_problem_model.md) — the strategic
  target and roadmap placement; issue 3 (serialization/data movement) and the closed
  type set.
* [rayx_runtime_hpx_design_principles.md](rayx_runtime_hpx_design_principles.md) — P6
  (closed typed arg/result set), P9 (rows separate from values), P10 (granularity /
  no micro-timing), P11 (GIL-free workers).
* [rayx_phase1_registered_operation_api.md](rayx_phase1_registered_operation_api.md) —
  the Phase 1 registry, the 9-field row, and the `int64`-only value path this note
  extends.
* [rayx_runtime_internal_composition_note.md](rayx_runtime_internal_composition_note.md)
  — `fanout_sum`, the composition extension that motivated confronting the value
  shape.
