# ChronicleFlow

ChronicleFlow is a small backend for durable workflow orchestration. It stores
workflow definitions, execution state, and an append-only event history in
SQLite so an execution can be inspected and replayed deterministically.

The initial release intentionally supports a compact public contract:

- workflows are directed acyclic graphs of `task`, `condition`, and bounded
  `loop` nodes;
- executions advance one ready task at a time, evaluating conditions and
  skipping unmatched branches automatically;
- loop nodes repeat their body a bounded number of times, re-evaluating a
  continue condition at the loop boundaries;
- every state transition is appended to the execution event stream;
- replay rebuilds execution state from the recorded events;
- duplicate commands with the same idempotency key return the original result;
- task nodes may declare a retry budget, so a failed task is re-attempted a
  bounded number of times before the execution fails permanently;
- executions may declare a timeout measured from start, and may be cancelled;
  failure, timeout, and completion each produce distinct terminal states.

## Requirements

- Python 3.11 or newer
- no third-party runtime dependencies

## Run the service

```bash
PYTHONPATH=src python -m chronicleflow.server --host 127.0.0.1 --port 8080 --database chronicleflow.db
```

The process prints `ChronicleFlow listening on http://127.0.0.1:8080` after it
has bound the port.

## HTTP API

All request and response bodies are JSON. Unknown fields are rejected.

### Health

```http
GET /health
```

Returns `{"status":"ok"}`.

### Create a workflow

```http
POST /workflows
Idempotency-Key: workflow-request-1

{
  "id": "order-flow",
  "nodes": [
    {"id": "reserve", "kind": "task", "depends_on": []},
    {"id": "charge", "kind": "task", "depends_on": ["reserve"]}
  ]
}
```

Returns HTTP 201 with the stored workflow. Node identifiers must be unique,
dependencies must exist, and cycles are rejected.

Besides `task`, a node may have `kind` set to `condition`:

```json
{"id": "is_vip", "kind": "condition", "depends_on": ["reserve"], "path": "customer.vip", "equals": true}
```

`path` is a non-empty dot-separated path into the execution input and `equals`
is a JSON scalar (string, number, boolean, or null). A condition is evaluated
automatically once its dependencies complete: the input value at `path` is
compared with `equals` by JSON type and value, and a missing path evaluates to
`false`.

A task may carry an optional `run_if` guard:

```json
{"id": "expedite", "kind": "task", "depends_on": ["is_vip"], "run_if": {"condition_id": "is_vip", "expected": true}}
```

`condition_id` must reference a `condition` node that is also listed in the
task's `depends_on`. When the condition's result differs from `expected`, the
task is marked as skipped: it receives no output and still satisfies the
dependencies of its successors.

A task may declare a `retries` budget:

```json
{"id": "charge", "kind": "task", "depends_on": ["reserve"], "retries": 2}
```

`retries` is an integer between 0 and 10 and defaults to 0. With the default
the task is attempted exactly once. When an attempt fails (see reporting
failures below), the task records a failure and, while attempts remain, goes
back to pending so the next `advance` re-drives it. A task with `retries` set
to 2 is attempted up to three times (the initial attempt plus two retries).
Exhausting the budget without a success fails the node permanently and
terminates the execution.

### Task timeouts and cancellation

An execution may declare a timeout, measured in seconds from its start:

```http
POST /executions
Idempotency-Key: execution-request-1

{"id":"run-1","workflow_id":"order-flow","input":{"order_id":"o-7"},"timeout_seconds":60}
```

`timeout_seconds` is a positive finite number. The timeout is measured from
start; on the next `advance`, `cancel`, inspection, event read, or replay once
the deadline has passed, the execution is terminated with reason `timed_out`
and a submitted output is not consumed. After that the execution no longer
accepts output, but its state and event stream remain queryable and replay
stays consistent.

An execution may also be cancelled explicitly:

```http
POST /executions/run-1/cancel
Idempotency-Key: cancel-request-1
```

The request body is optional and, when present, must be an empty JSON object.
Cancellation takes effect immediately on an execution that is still running,
terminating it with reason `cancelled`. Cancelling a completed or otherwise
terminated execution returns that execution unchanged. Cancelling an unknown
execution returns 404.

There is a single terminal reason on the state, `termination_reason`, set to
one of `completed`, `failed`, `cancelled`, or `timed_out`; `status` takes the
same terminal value. The failure, cancellation, and timeout reasons are never
confused with completion.

A node may also have `kind` set to `loop`, describing a bounded repeated
segment:

```json
{"id": "retry", "kind": "loop", "depends_on": ["reserve"], "entry": "attempt", "condition": "keep_trying", "max_iterations": 3}
```

A loop node contains exactly `id`, `kind`, `depends_on`, `entry`, `condition`,
and `max_iterations`; any other field is rejected as unknown. `entry` names a
`task` node, `condition` names a `condition` node, and `max_iterations` is an
integer between 1 and 100. The loop body is the entry task plus every node
reachable from it through `depends_on`; the body keeps the usual DAG rules and
must contain the referenced condition. The loop's own dependencies must be
completed or skipped before the first iteration may start, they must not
overlap the body, bodies of different loops must not overlap or nest, and
nodes outside a body must not depend on nodes inside it (they depend on the
loop node instead).

When the loop's dependencies are satisfied, the loop evaluates its condition
against the execution input (same JSON type and value comparison as condition
nodes; a missing path is `false`). If it is `false`, the loop completes with
zero iterations and end reason `condition_false`. If it is `true`, the first
iteration starts and the body advances one ready task per `advance` call in
the usual deterministic order; conditions and `run_if` skips inside the body
are re-evaluated every iteration, and task outputs belong only to the current
iteration. Once every body node is completed or skipped, the condition is
evaluated again: `true` starts the next iteration, `false` ends the loop with
`condition_false`, and reaching `max_iterations` ends it with
`iteration_limit` after that iteration finishes. Ending the loop completes
the loop node and releases the dependencies of its successors.

A task inside a loop body follows the same retry and termination rules as any
other task: its attempt and failure counters are tracked per iteration and
reset to a fresh first attempt when a new iteration starts, and exhausting a
body task's retries terminates the whole execution. Iteration output ownership
and the existing loop semantics are otherwise unchanged.

Execution state exposes each loop under `loops`: `status`, the
`current_iteration`, one entry per iteration with its own `completed_nodes`,
`skipped_nodes`, `condition_results`, and `outputs`, and the single
`end_reason` (`condition_false` or `iteration_limit`). The event stream
records `iteration_started`, `loop_condition_evaluated`, and `loop_completed`
events alongside the usual per-node ones, so replay rebuilds loop state
exactly.

### Start an execution

```http
POST /executions
Idempotency-Key: execution-request-1

{"id":"run-1","workflow_id":"order-flow","input":{"order_id":"o-7"}}
```

Returns HTTP 201. The execution starts in `running` state.

### Inspect an execution

```http
GET /executions/run-1
GET /executions/run-1/events
```

The first endpoint returns the materialized state. The second returns the
ordered event stream.

### Complete the next ready node

```http
POST /executions/run-1/advance
Idempotency-Key: advance-request-1

{"output":{"reservation_id":"r-9"}}
```

The lexicographically first ready task is completed. The response contains the
updated execution. Before that, each call first evaluates all ready conditions
in deterministic order and skips tasks whose `run_if` does not match; if this
automatic processing finishes the execution, the current state is returned and
the submitted output is not consumed. When every node is completed or skipped,
the status becomes `completed`.

Instead of an output, an advance may report that the ready task failed:

```http
POST /executions/run-1/advance
Idempotency-Key: advance-request-1

{"failure":{"reason":"payment gateway rejected the charge"}}
```

`reason` is a non-empty string. The node enters a failed state and appends a
`node_failed` event carrying the 1-based `attempt` and the failure `reason`. If
the node still has retries left, a `node_retried` event is appended and the
node goes back to pending to be driven again on the next `advance`. Each retry
appends its own events, so every attempt and its failure reason are present in
order in the stream. When the retry budget is exhausted, the node is failed
permanently, an `execution_terminated` event with reason `failed` is appended,
and the execution stops advancing.

Execution state includes `completed_nodes` (which also lists evaluated
conditions), `skipped_nodes`, `condition_results`, and `outputs`. Each
condition evaluation appends a `condition_evaluated` event and each skip a
`node_skipped` event to the execution stream.

When at least one task declares retries, the state also carries a `nodes` map
(with per-iteration entries inside each loop iteration for body tasks). Each
tracked node reports its current `attempt` (the 1-based attempt that is
running or last ran), its number of `failures`, and a `status` of `pending`,
`failed`, `skipped`, or `completed`. Terminal executions additionally expose
`termination_reason`, and timed-out executions expose `timeout_seconds` and
`started_at`. The order of all tracking entries matches the event stream.

Advancing a terminated execution (completed, failed, cancelled, or timed out)
returns the current state unchanged and does not absorb the submitted output or
failure.

### Replay

```http
POST /executions/run-1/replay
```

Rebuilds state solely from the execution event stream and compares it with the
stored materialized state. A successful response contains `consistent: true`
and the rebuilt execution.

## Errors

Errors use this shape:

```json
{"error":{"code":"validation_error","message":"human readable detail"}}
```

Validation errors return 400, missing resources return 404, and conflicts
return 409. Invalid `retries` values (negative, non-integer, or greater than
10) and non-positive `timeout_seconds` values return 400 `validation_error`.
Duplicate workflow or execution identifiers, and idempotency keys reused
across operations, return 409 `conflict`; operations on a missing workflow or
execution, including cancelling a missing execution, return 404 `not_found`.
Request bodies must not contain non-finite numbers (`NaN`,
`Infinity`, or overflowing values such as `1e400`); they are rejected with
400. Finite floats keep their full precision, including negative zero
(`-0.0`), and every response body ends with a single newline.

## Tests

```bash
PYTHONPATH=src python -m unittest discover -s tests -v
```

