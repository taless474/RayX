# Project Proposal: HPX as a Distributed Execution Substrate for ML Serving Workloads

## Motivation

Ray and HPX both provide models for distributed execution. Ray exposes this mainly through a Python-first ecosystem with tasks, actors, object references, scheduling, and integrations such as Ray Serve and Ray Train. HPX provides an asynchronous many-task runtime in C++ with futures, actions, components, localities, and a global address space. Conceptually, Ray’s actor/task model and HPX’s distributed components/actions overlap at the level of distributed execution.

The question for this project is not simply “which small part of Ray should HPX replace?” A better question is:

**For ML serving and inference-control workloads, where can HPX’s C++ asynchronous distributed runtime provide a clear advantage over Ray’s actor-based distributed runtime?**

This project investigates HPX as an alternative or complementary distributed execution substrate for native, latency-sensitive ML serving workloads.

## Core Hypothesis

HPX may provide value where the serving workload needs:

1. Low-overhead C++ task scheduling.
2. Fine-grained asynchronous execution.
3. Native futures/promises instead of Python-level orchestration.
4. Efficient coordination of request lifecycle, cancellation, backpressure, and streaming.
5. Distributed actor-like execution without relying on a Python-first control plane.
6. Better fit for HPC/native runtimes where model execution and serving control are both C++-centric.

Ray is highly useful for Python ML ecosystems and broad distributed orchestration. HPX may be stronger when the workload is lower-level, native, latency-sensitive, and needs tighter integration with C++ inference runtimes.

## Scope

This project will compare Ray-style distributed serving against an HPX-native serving-control design.

The goal is not to replace model kernels, GPU runtimes, tensor libraries, or inference math. The model backend should remain opaque.

HPX would own the serving-control layer:

* Request admission
* Queueing
* Scheduling
* Actor-like worker ownership
* Futures/promises
* Cancellation
* Backpressure
* Streaming token delivery
* Metrics and tracing
* Optional distributed placement across HPX localities

The model execution backend remains separate:

* llama.cpp
* vLLM
* SGLang
* TensorRT-LLM
* OpenVINO GenAI
* or a synthetic model backend for controlled experiments

## Main Research Question

**Can HPX provide a lower-overhead, more native C++ distributed execution substrate for inference-serving control loops than a Ray-style actor/task architecture?**

More concretely:

* Where does Ray’s actor/task overhead matter?
* Where does HPX’s many-task runtime help?
* Does HPX improve latency, throughput, tail latency, cancellation responsiveness, or control-plane scalability?
* Is the advantage local-only, distributed-only, or both?
* What workloads make HPX look better?
* What workloads make Ray the better choice?

## Proposed Architecture

```text
Client requests
      |
      v
Serving frontend
      |
      v
HPX serving control plane
  - request queue
  - admission policy
  - actor-like session/request ownership
  - futures/promises
  - cancellation
  - backpressure
  - streaming response channel
  - metrics/tracing
      |
      v
Opaque inference backend
  - llama.cpp / synthetic backend / other engine
      |
      v
Token/result stream
```

A Ray-style comparison architecture would be:

```text
Client requests
      |
      v
Ray Serve / Ray actor layer
  - actor placement
  - task submission
  - object refs
  - Python control plane
      |
      v
Native or Python-backed inference worker
      |
      v
Token/result stream
```

The important comparison is not just API style. The comparison is about control-plane cost, scheduling granularity, responsiveness, and how efficiently each runtime manages many concurrent requests.

## Phase 1: Minimal Local Prototype

Start with one node and one model backend.

Build a small HPX-native serving prototype with:

* One long-lived engine object.
* A request queue.
* Per-request futures/promises.
* A simple admission policy.
* A fixed maximum number of active sequences or active jobs.
* Streaming output simulation or real token streaming.
* Cancellation support.
* Basic metrics:

  * time to first token/result
  * total latency
  * queue wait time
  * active request count
  * completed/cancelled requests
  * p50/p90/p99 latency

Use either:

1. A synthetic backend first, to isolate runtime overhead.
2. llama.cpp second, to test realistic inference-control behavior.

The first prototype should avoid distributed complexity. It should prove whether HPX is useful as a local serving-control runtime.

## Phase 2: Ray Baseline

Build a comparable Ray actor baseline.

The Ray baseline should match the HPX prototype as closely as possible:

* Same request shape.
* Same synthetic backend or same model backend.
* Same concurrency limits.
* Same request arrival pattern.
* Same output streaming/cancellation semantics if possible.
* Same metrics.

The goal is not to benchmark Ray unfairly. The goal is to identify where Ray’s actor/task model is a good fit and where HPX’s C++ runtime may have an advantage.

## Phase 3: Workload Matrix

Test several workload shapes:

### 1. Short requests

Many short requests with small compute time.

This stresses scheduling overhead, queueing, wakeups, and per-request control-plane cost.

### 2. Long requests

Fewer long-running requests.

This tests whether runtime overhead disappears when model compute dominates.

### 3. Streaming requests

Requests produce incremental outputs.

This tests future/promise behavior, streaming overhead, and cancellation responsiveness.

### 4. Bursty arrivals

Requests arrive in bursts rather than steady-state.

This tests admission control and queue stability.

### 5. Cancellation-heavy workload

Some requests are cancelled while queued or mid-execution.

This tests how naturally each runtime handles request lifecycle and cleanup.

### 6. Distributed workers

Multiple workers across localities/nodes.

This tests whether HPX’s distributed model can act as a Ray-like execution substrate in a native C++ environment.

## Phase 4: Distributed HPX Design

After the local prototype works, extend the HPX design to multiple localities.

Possible distributed design:

```text
Coordinator locality
  - receives requests
  - tracks load
  - assigns requests to worker localities
  - manages global admission/backpressure

Worker locality
  - owns one or more inference engines
  - runs local HPX serving loop
  - streams results back
  - reports metrics
```

This would make HPX closer to Ray Core conceptually:

```text
Ray actor/task model
        vs.
HPX localities + components + actions + futures
```

The project should compare whether HPX can express the same distributed serving pattern with lower overhead or better C++ integration.

## What HPX Should Not Replace Initially

This project should not start by replacing:

* Ray’s full Python ecosystem
* Ray Serve frontend APIs
* Ray Train
* RLlib
* Object store semantics
* Full cluster management
* Fault tolerance layer
* GPU kernels
* Tensor execution backend
* Model math

Those are too broad and would make the project unclear.

The first useful target is narrower:

**HPX as the native distributed execution/runtime layer for inference-serving control.**

## Success Criteria

The project is successful if it can clearly answer:

1. Does HPX reduce control-plane overhead compared with Ray actors for native serving workloads?
2. Does HPX improve p50/p90/p99 latency under short or bursty workloads?
3. Does HPX provide better cancellation or streaming responsiveness?
4. Does HPX scale cleanly from local execution to distributed execution?
5. Can HPX express actor-like serving patterns naturally using components/actions/futures?
6. Is the benefit large enough to justify using HPX instead of Ray for C++-native ML serving?

## Expected Outcome

The expected outcome is not necessarily “HPX beats Ray everywhere.”

A more realistic expected outcome is:

* Ray remains better for Python-first ML orchestration and ecosystem integration.
* HPX may be better for native C++ serving runtimes, fine-grained scheduling, low-overhead asynchronous control, and HPC-style distributed execution.
* The most promising design is HPX as a C++ distributed serving substrate, either replacing Ray Core in native deployments or living underneath a higher-level ML serving API.

## Initial Implementation Plan

### Step 1

Build a local HPX serving-control prototype with a synthetic backend.

### Step 2

Add metrics and deterministic correctness checks.

### Step 3

Create a matching Ray actor baseline.

### Step 4

Compare short, long, bursty, streaming, and cancellation-heavy workloads.

### Step 5

Replace the synthetic backend with a real llama.cpp-backed engine.

### Step 6

Extend HPX to multiple localities and compare distributed actor-like execution.

## One-Sentence Project Summary

This project investigates whether HPX can serve as a low-overhead C++ distributed execution substrate for ML inference-serving control loops, offering a native alternative to Ray’s actor/task model for latency-sensitive and HPC-style workloads.
