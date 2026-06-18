// rayx Phase 2 runtime: fixed local native actor registry (HPX-free layer).
//
// This header is part of the EXPERIMENTAL `rayx.runtime` prototype. It is the
// Slice B "actor registry layer" companion to runtime_ops.hpp: a fixed,
// C++-side registry of ADDRESSABLE STATEFUL native actor types, each with a
// closed set of registered native methods. Like runtime_ops.hpp it is
// deliberately HPX-FREE -- it defines only the type set and registry; the lane
// ownership, per-method task closure, actor map, and Python boundary live in
// _rayx.cpp (Slice B wiring, not in this header yet).
//
// It defines NO Python callables and NO arbitrary execution: an actor type id +
// method id resolves to a pre-compiled native functor over the SAME closed value
// channel as operations (OpArgs / OpValue = variant<int64,double>). Actor STATE
// is native C++ struct data, NOT an OpValue bag -- the value channel is for
// method args/results only (see docs/design/rayx_runtime_local_native_actors.md,
// "Native actor state model"). NO object store, NO pickle, NO Python state, NO
// HPX actions/components/localities, NO dynamic method registration, and NO
// performance claim.
//
// Reuses runtime_ops.hpp's OpArgs / OpValue / OpOutcome / OpType / StopCheckpoint
// / as_int64 / one_checkpoint unchanged: method calls return the existing
// OpOutcome and flow through the existing RuntimeFuture / get / wait /
// as_completed with no new collection code.

#ifndef RAYX_RUNTIME_ACTOR_OPS_HPP
#define RAYX_RUNTIME_ACTOR_OPS_HPP

#include <cstdint>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "runtime_ops.hpp"  // OpArgs, OpValue, OpOutcome, OpType, StopCheckpoint,
                            // as_int64, one_checkpoint -- reused unchanged.

namespace rayx_runtime {

// Abstract base giving the registry a uniform handle to per-type native state.
// Each concrete type keeps strongly-typed native fields and reports a stable
// type_id() used for the defensive downcast backstop (see as_counter). State is
// NEVER an OpValue blob -- that would drift toward an object store.
struct ActorState {
    virtual ~ActorState() = default;
    virtual const char* type_id() const = 0;  // defensive identity check
};

// `counter` actor state: a single native std::int64_t, mutated FIFO per actor.
struct CounterState : ActorState {
    std::int64_t v = 0;
    const char* type_id() const override { return "counter"; }
};

// Defensive downcast backstop, mirroring as_int64 for args: the PUBLIC path only
// ever dispatches a method onto the matching actor type, so this always succeeds
// there. It is the NATIVE guard for the private/raw bypass -- a wrong tag throws
// std::invalid_argument, which _rayx.cpp maps to a status="failed" row, never a
// crash. The cast is safe only after the type_id() match.
inline CounterState& as_counter(ActorState& st, const char* method) {
    if (std::string(st.type_id()) != "counter")
        throw std::invalid_argument(std::string("method '") + method +
            "' called on non-counter actor state");
    return static_cast<CounterState&>(st);
}

// Registered actor method signature: native state + typed args (the closed
// OpArgs channel) + a lane-bound StopCheckpoint in, OpOutcome out -- the SAME
// outcome type operations return. Args are arity/type-validated at the Python
// boundary and marshalled into OpArgs in _rayx.cpp; the body extracts each arg
// with as_int64/as_double (defensive backstop for the raw bypass) and downcasts
// state with as_counter. MVP methods are instantaneous and ignore `stop`.
using MethodFn = std::function<
    OpOutcome(ActorState& st, const OpArgs& args, const StopCheckpoint& stop)>;

struct MethodEntry {
    int arity;
    std::vector<OpType> arg_types;  // typed signature, exactly `arity` entries
    OpType result_type;             // declared result tag
    // Number of cancellation checkpoints this method will reach (one_checkpoint
    // for every MVP method -> count 1 -> queued-cancel only, never running-cancel
    // of a state mutation; see the design note). Same defensive contract as the
    // op registry: runs BEFORE the task/future exist.
    std::function<int(const OpArgs& args)> checkpoint_count;
    MethodFn fn;                    // downcasts ActorState defensively
    // Internal lane-dispatch policy (last field, defaulted), reusing the op
    // registry's DispatchPolicy. Instantaneous methods (add/get/reset) opt into
    // Inline; the checkpointed busy_get inherits the conservative Async default.
    DispatchPolicy policy = DispatchPolicy::Async;
};

struct ActorTypeEntry {
    std::vector<OpType> init_arg_types;  // init() signature; validated at boundary
    // Builds native state synchronously, HPX-free, on the Python thread before
    // any actor lane exists (so a future factory failure raises before a worker
    // thread is spawned). CounterState's factory cannot fail.
    std::function<std::unique_ptr<ActorState>(const OpArgs& args)> factory;
    std::unordered_map<std::string, MethodEntry> methods;
};

// The fixed actor registry, built once. MVP ships exactly one type, `counter`,
// with init(int64) and methods add/get/reset -- proving state persistence across
// calls, per-actor FIFO ordering, instance independence, a read method, and two
// distinct typed mutators -- plus busy_get(int64), a READ-ONLY CHECKPOINTED method
// that performs synthetic on-core diagnostic/calibration work (the actor-method
// analog of the op-level busy_sum) and returns the CURRENT value unchanged. It
// exercises the armed checkpoint/running-cancel path through actor dispatch without
// mutating state; it is NOT a benchmark and carries NO performance claim. NO Python
// callable, NO dynamic registration.
inline const std::unordered_map<std::string, ActorTypeEntry>& actor_registry() {
    static const std::unordered_map<std::string, ActorTypeEntry> r = {
        {"counter", ActorTypeEntry{
             {OpType::Int64},  // init(int64 initial)
             [](const OpArgs& a) -> std::unique_ptr<ActorState> {
                 auto st = std::make_unique<CounterState>();
                 st->v = as_int64(a, 0, "counter.init");
                 return st;
             },
             {
                 // add(int64 delta) -> int64: mutate state, return the NEW value.
                 {"add", MethodEntry{1,
                      {OpType::Int64}, OpType::Int64,
                      one_checkpoint,
                      [](ActorState& st, const OpArgs& a, const StopCheckpoint&) {
                          CounterState& c = as_counter(st, "add");
                          c.v += as_int64(a, 0, "add");
                          OpOutcome o;
                          o.value = c.v;  // int64 -> OpValue
                          o.has_value = true;
                          return o;
                      }, DispatchPolicy::Inline}},
                 // get() -> int64: read current state.
                 {"get", MethodEntry{0,
                      {}, OpType::Int64,
                      one_checkpoint,
                      [](ActorState& st, const OpArgs&, const StopCheckpoint&) {
                          CounterState& c = as_counter(st, "get");
                          OpOutcome o;
                          o.value = c.v;
                          o.has_value = true;
                          return o;
                      }, DispatchPolicy::Inline}},
                 // reset(int64 value) -> int64: overwrite state, return new value.
                 {"reset", MethodEntry{1,
                      {OpType::Int64}, OpType::Int64,
                      one_checkpoint,
                      [](ActorState& st, const OpArgs& a, const StopCheckpoint&) {
                          CounterState& c = as_counter(st, "reset");
                          c.v = as_int64(a, 0, "reset");
                          OpOutcome o;
                          o.value = c.v;
                          o.has_value = true;
                          return o;
                      }, DispatchPolicy::Inline}},
                 // busy_get(int64 work_n) -> int64: READ-ONLY synthetic on-core
                 // diagnostic/calibration work (the actor-method analog of the
                 // op-level busy_sum), then return the CURRENT counter value
                 // UNCHANGED. The work is the SHARED run_masked_checkpoint_loop
                 // from runtime_ops.hpp -- the same loop busy_sum runs (same
                 // BUSY_SUM_STRIDE / busy_sum_checkpoints / per-chunk StopCheckpoint
                 // poll, with the lane providing the cooperative yield), so a
                 // work_n > BUSY_SUM_STRIDE arms running-cancellability
                 // (checkpoint_count > 1) through the actor dispatch path. It NEVER
                 // writes c.v: the loop's masked accumulator is discarded into a
                 // volatile sink purely so the read-only work cannot be elided, and
                 // the RESULT is always c.v. A running cancel returns
                 // status="cancelled" with no value and leaves c.v untouched. NO
                 // sleep, NO state mutation, NO performance claim. work_n >= 0 is
                 // guaranteed by the Python boundary (validate_actor_call).
                 {"busy_get", MethodEntry{1,
                      {OpType::Int64}, OpType::Int64,
                      // Defensive checkpoint_count (runs BEFORE the task/future
                      // exist, so it cannot produce a failed row): the real count on
                      // the valid int64 tag, else 1 (queued-cancelable only), letting
                      // the body's as_int64 produce the failed row on a wrong tag.
                      // Mirrors busy_sum's checkpoint_count lambda.
                      [](const OpArgs& a) {
                          return is_int64(a, 0)
                              ? busy_sum_checkpoints(std::get<std::int64_t>(a[0]))
                              : 1;
                      },
                      [](ActorState& st, const OpArgs& a, const StopCheckpoint& stop)
                          -> OpOutcome {
                          CounterState& c = as_counter(st, "busy_get");
                          const std::int64_t n = as_int64(a, 0, "busy_get");
                          std::uint64_t acc = 0;
                          if (!run_masked_checkpoint_loop(n, stop, acc)) {
                              OpOutcome o;  // honored running cancel at boundary
                              o.has_value = false;
                              o.status = "cancelled";
                              return o;     // c.v untouched -- read-only
                          }
                          // Discard the synthetic work so the read-only loop is
                          // genuine on-core work (not dead-code-eliminated); the
                          // RESULT is the CURRENT counter value, never acc.
                          volatile std::uint64_t sink = acc;
                          (void)sink;
                          OpOutcome o;
                          o.value = c.v;  // read current state, UNCHANGED
                          o.has_value = true;
                          o.status = "completed";
                          return o;
                      }}},
             }}},
    };
    return r;
}

}  // namespace rayx_runtime

#endif  // RAYX_RUNTIME_ACTOR_OPS_HPP
