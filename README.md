# ChronicleFlow

ChronicleFlow is a small backend for durable workflow orchestration. It stores
workflow definitions, execution state, and an append-only event history in
SQLite so an execution can be inspected and replayed deterministically.

The initial release intentionally supports a compact public contract:

- workflows are directed acyclic graphs of `task`, `condition`, and bounded
  `loop` nodes;
- executions advance one ready task at a time, evaluating conditions and
  skipping unmatched branches automatically;
- task nodes may declare a bounded number of retries: a submitted failure
  re-queues the node until the retries are exhausted, which terminates the
  execution;
- executions may declare a timeout in seconds, after which they terminate and
  no longer accept output, and they may be cancelled explicitly;
- loop nodes repeat their body a bounded number of times, re-evaluating a
  continue condition at the loop boundaries;
- every state transition is appended to the execution event stream;
- replay rebuilds execution state from the recorded events;
- a checkpoint is written at every node boundary, capturing the state
  summary and event position, so an execution can be recovered from the
  latest checkpoint after a restart;
- workers may claim a running execution to receive a work item with a
  time-bounded lease, renew the lease with heartbeats, and release it;
  while a lease is active only its holder may submit results, and an
  expired or released lease returns the work item to the claimable set;
- duplicate commands with the same idempotency key return the original result.

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

A task may declare how many times it is retried after a failure:

```json
{"id": "charge", "kind": "task", "depends_on": ["reserve"], "retries": 3}
```

`retries` is an integer between 0 and 10 and defaults to 0, meaning the task
is attempted only once. Each failure submitted through `advance` consumes one
attempt; while retries remain, the task returns to the ready set and is
advanced again. When a failure arrives with no retries left, the task is
permanently failed and the whole execution terminates with termination reason
`retries_exhausted`.

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

An execution may declare a timeout:

```json
{"id":"run-2","workflow_id":"order-flow","input":{},"timeout_seconds":30}
```

`timeout_seconds` is a positive number of seconds counted from the moment the
execution starts. Once the deadline passes, the execution terminates with
termination reason `timeout` and no longer accepts output. Executions without
a timeout never expire.

### Inspect an execution

```http
GET /executions/run-1
GET /executions/run-1/events
GET /executions/run-1/checkpoints
```

The first endpoint returns the materialized state. The second returns the
ordered event stream. The third returns the ordered checkpoints; each entry
gives its `sequence`, the `event_sequence` position it was taken at, the full
`state` summary, and `created_at`.

### Checkpoints

Every successful `advance` that settles a node boundary — a task is
completed, or a failure is submitted (whether it retries or exhausts the
attempts) — writes a checkpoint in the same transaction as the state update
and event append. The checkpoint stores the complete state summary at that
boundary, including completed, skipped, and failed nodes, condition results,
outputs, attempts with their unfinished retry counts, and the status and
current iteration of every loop (with the per-iteration records), together
with the position of the last written event. Because the checkpoint matches
the stored state at that position, recovery can continue without replaying or
duplicating any node output.

Checkpoints do not add fields to the execution state and do not append events:
an execution that declares no retries, timeout, or loops keeps exactly the
same state shape and event stream as before.

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

Instead of an output, a failure can be submitted for the same ready task:

```http
POST /executions/run-1/advance
Idempotency-Key: advance-request-2

{"failure":{"reason":"gateway timeout"}}
```

The body must contain exactly one of `output` or `failure`, and `failure`
carries exactly a `reason` string. Either body may additionally carry a
`worker_id` string identifying the lease holder when the execution's work
item has been claimed (see "Claim a work item"). A failed task appends a `node_failed`
event recording the attempt number and reason. While the task has retries
left it returns to the ready set and a `node_retried` event records the next
attempt number; otherwise the task is permanently failed and the execution
terminates with reason `retries_exhausted`. The same rules apply to tasks
inside loop bodies, which are retried within their current iteration.

Execution state includes `completed_nodes` (which also lists evaluated
conditions), `skipped_nodes`, `failed_nodes`, `condition_results`, `outputs`,
and `attempts`. `attempts` maps each attempted task to its current `attempt`
number and its `failures` count; loop body tasks track the same per iteration.
Each condition evaluation appends a `condition_evaluated` event and each skip
a `node_skipped` event to the execution stream.

### Cancel an execution

```http
POST /executions/run-1/cancel
Idempotency-Key: cancel-request-1
```

A running execution is terminated immediately with termination reason
`cancelled`. Cancelling a completed or already terminated execution returns
its state unchanged, and cancelling a missing execution returns 404.

Execution status is `running`, `completed`, or `terminated`. A terminated
execution records exactly one `termination_reason` — `retries_exhausted`,
`timeout`, or `cancelled` — and appends a single `execution_terminated`
event; completed executions keep a `null` termination reason and their own
`execution_completed` event. Advancing a terminated execution returns its
state unchanged without consuming the submitted output or failure.

### Claim a work item

```http
POST /executions/run-1/claim
Idempotency-Key: claim-request-1

{"worker_id":"worker-7","lease_seconds":30}
```

Claiming is how an external worker takes ownership of a running execution
before advancing its ready tasks. `worker_id` is a non-empty string and
`lease_seconds` is an optional positive number of seconds (default 30). The
response contains the claimed `work_item` (its `execution_id` and
`workflow_id`) and a `lease` recording the `worker_id`, the `lease_seconds`
duration, the `expires_at` deadline, and the `heartbeat_at` active time.
Each work item is held by at most one worker at a time: claiming a work
item whose lease is still active — even by the same worker — is a 409
`conflict`. Claiming a completed or terminated execution returns the
definite empty result `{"work_item":null,"lease":null}` and absorbs no
input, and claiming a missing execution returns 404.

While a work item is held, results are submitted through the usual
`advance` entry by including the holder's identity:

```http
POST /executions/run-1/advance
Idempotency-Key: advance-request-3

{"output":{"reservation_id":"r-9"},"worker_id":"worker-7"}
```

Submitting results for a work item held by another worker, or after the
lease has expired, is a 409 `conflict` that does not change execution
state. An execution that never claimed a work item accepts `advance`
exactly as before, and its state fields and event stream are identical to
an execution without any claim.

When a lease expires, the work item returns to the claimable set and may be
claimed again by the same or another worker. The second claim continues
from the materialized state: node outputs and results recorded by the
earlier holder are never advanced or recorded twice, and every written
output belongs to the single advance that submitted it.

### Renew a lease

```http
POST /executions/run-1/heartbeat
Idempotency-Key: heartbeat-request-1

{"worker_id":"worker-7"}
```

Within the lease period the holder may heartbeat to extend `expires_at` by
the lease duration and refresh the `heartbeat_at` active time; the response
carries the updated `lease`. A heartbeat never advances nodes, writes
outputs, or appends node events. A heartbeat from another worker or after
the lease expired is a 409 `conflict`; a heartbeat for a missing execution
or an execution with no claimed work item is a 404 `not_found`.

### Release a work item

```http
POST /executions/run-1/release
Idempotency-Key: release-request-1

{"worker_id":"worker-7"}
```

Releasing invalidates the lease immediately and returns the work item to
the claimable set, so it can be claimed again right away; progress already
made and the recorded events are unchanged. Releasing with another
worker's identity or after expiry is a 409 `conflict`; releasing for a
missing execution or an execution with no claimed work item is a 404
`not_found`.

Claims, heartbeats, and releases append no events and add no fields to the
execution state, so replay, checkpoints, and recovery are unaffected.
Leases live in the same SQLite database as the execution state, so
unexpired leases and claim ownership remain valid after the service
restarts.

### Recover from a checkpoint

```http
POST /executions/run-1/recover
Idempotency-Key: recover-request-1

{"from":"latest_checkpoint"}
```

Recovery rebuilds a running execution from its latest checkpoint and returns
the rebuilt execution. It appends no events and changes no state: the rebuilt
execution is exactly the materialized state, and a subsequent replay has the
same conclusion as before the interruption. After recovery, further
`advance` calls continue from the checkpoint, so node outputs, failure
reasons, attempt numbers, and loop iteration ownership are identical to an
uninterrupted run; failure records for tasks inside a loop body still carry
their loop id and current iteration. Recovery works after the service has
been restarted, because checkpoints live in the same SQLite database as the
execution state and events.

A completed execution is returned unchanged, producing no new events. A
terminated execution is likewise returned unchanged and the operation
absorbs no input; if a timeout or cancellation takes effect between
checkpoint writes, termination takes precedence over recovery. Events and
checkpoints remain queryable after cancellation or timeout, and their replay
stays consistent with the materialized state.

Recovering a missing execution returns 404. Recovering an execution that has
no checkpoint, or whose latest checkpoint cannot be parsed, returns 409
`conflict`. A missing `from` field, a wrong type, or an unknown recovery
origin is a 400 `validation_error`; a non-finite number in the body is
rejected the same way as every other request. Reusing an idempotency key
already used by another operation (including an `advance` or `cancel` on the
same execution) returns 409 `conflict`.

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
return 409. A `retries` value that is negative, non-integer, or greater than
10, and a non-positive `timeout_seconds`, are validation errors. Reusing a
workflow or execution identifier, or reusing an idempotency key across
different operations, is a conflict. Claiming a work item whose lease is
still active, submitting results for a work item held by another worker or
after the lease expired, and heartbeating or releasing a lease held by
another worker are conflicts, while heartbeating or releasing an execution
with no claimed work item is a missing resource. Recovering a missing
execution is a missing resource, while recovering an execution that has no
checkpoint or whose latest checkpoint is unparseable is a conflict; an
invalid recover body is a validation error. Request bodies must not contain
non-finite numbers (`NaN`, `Infinity`, or overflowing values such as `1e400`);
they are rejected with 400. Finite floats keep their full precision, including negative zero
(`-0.0`), and every response body ends with a single newline.

## Tests

```bash
PYTHONPATH=src python -m unittest discover -s tests -v
```

