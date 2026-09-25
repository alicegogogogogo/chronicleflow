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
- a task node may declare an approval point with the people allowed to decide
  it; advancing parks the task in a waiting state until an approver approves
  (completing it with the submitted output) or rejects (terminating the
  execution with reason `rejected`);
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
- duplicate commands with the same idempotency key return the original result;
- a workflow may declare a schedule (a fixed interval or a five-field cron
  rule) with its own input and a missed-period policy, and the service creates
  an execution automatically whenever a period comes due;
- workflows and executions may declare webhook subscriptions, and matching
  business events are delivered to the declared targets with bounded retries,
  each delivery recorded in a per-execution history.

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

A workflow may also declare webhook `subscriptions` alongside its nodes:

```json
{
  "id": "order-flow",
  "nodes": [{"id": "reserve", "kind": "task", "depends_on": []}],
  "subscriptions": [{"url": "https://hooks.example.com/orders", "events": ["execution_completed"]}]
}
```

See "Webhook notifications" below for the subscription shape and delivery
semantics; the subscriptions apply to every execution of the workflow.

A workflow may also declare a `schedule` here; see "Scheduled executions"
below. A stored workflow echoes its declared schedule, and only workflows with
a schedule carry that field.

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

A task may instead declare an approval point:

```json
{"id": "charge", "kind": "task", "depends_on": ["reserve"], "approval": {"approvers": ["alice", "bob"]}}
```

`approval` contains exactly `approvers`, a non-empty array of approver
identifier strings without duplicates; an empty list, a duplicate entry, or a
non-string entry is a validation error, and only `task` nodes may carry an
approval point. When an `advance` reaches a ready task with an approval point,
the task does not complete and the submitted output or failure is not
consumed: an `approval_requested` event is appended (recording the node, its
approvers, and, inside a loop body, the loop id and iteration) and the
execution stays `running`, parked in a waiting state returned under
`waiting_approval`. Further `advance` calls return the current state without
writing output, completing the node, or appending events; only a decision can
move the task.

### Submit an approval decision

```http
POST /executions/run-1/decision
Idempotency-Key: decision-request-1

{"approver":"alice","decision":"approved","output":{"charge_id":"c-3"}}
```

`decision` is either `approved` or `rejected`. An approved decision carries
exactly `approver`, `decision`, and an `output` object; the task completes with
that output and its successors become available in the usual way. A rejected
decision carries exactly `approver`, `decision`, and a `reason` string; the
task is permanently failed (it is listed under `failed_nodes`) and the
execution terminates with termination reason `rejected`. Either decision
appends an `approval_decided` event with the node, approver, decision, and (on
rejection) the reason, and the approval record is kept under `approvals`. A
request whose approver is not in the point's approver list is a 409
`conflict`, and a malformed body or a decision value other than `approved` or
`rejected` is a 400 `validation_error`; neither changes execution state.
Repeating the decision that already resolved the most recent approval point —
the same approver and decision with the same output or rejection reason —
returns the first result and neither advances the node again nor appends
another event; any other decision against an execution with no pending
approval point is a 409 `conflict`, and deciding a missing execution returns
404.

While an execution is parked at an approval point it remains `running`, so a
cancellation takes effect immediately with the usual `cancelled` termination
reason, and a reached timeout terminates it with `timeout`; both dismiss the
pending point and the execution then no longer accepts decisions or output.
The waiting point, its request and decision events, and the termination
reason are rebuilt solely from the event stream on replay, and the parked and
decided node boundaries are checkpointed like every other boundary, so
waiting points and recorded decisions remain valid after a restart. Approval
state is added to an execution only when its workflow declares at least one
approval point; workflows without approvals keep exactly the previous state
shape and event stream.

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

### Scheduled executions

A workflow may declare a schedule when it is created, or later through a
dedicated operation. The declaration carries the plan, the input every
scheduled execution starts with, and the policy for periods the service was
not awake to handle:

```json
"schedule": {
  "interval_seconds": 300,
  "input": {"source": "timer"},
  "misfire_policy": "catch_up"
}
```

Exactly one of `interval_seconds` (a positive integer number of seconds) or
`cron` (a five-field cron expression) is allowed; declaring both or neither is
a validation error. `misfire_policy` is exactly `catch_up` or `skip`. A cron
expression has five whitespace-separated fields — minute (0-59), hour (0-23),
day of month (1-31), month (1-12), day of week (0-6, Sunday through Saturday)
— and supports `*`, comma lists, numeric ranges (`9-17`), and steps (`*/15`,
`10-20/2`); a wrong field count, an out-of-range value, or an unparseable
fragment rejects the request. When both day fields are restricted a day
matches on either rule; when one is `*` the other decides alone.

The schedule takes effect as soon as the workflow exists. When a period comes
due the service creates exactly one execution for the workflow with the
declared input; that execution advances, awaits approval, holds leases,
retries, times out, and cancels exactly like a manually created one — its
state shape and event stream are identical (the trigger writes nothing into
them). Period creation is idempotent within a period: repeated triggers and
repeated requests for the same period return the same execution and never
create a second one. If periods were missed (for example while the service
was down), `catch_up` runs only the single most recently missed period and
`skip` runs none of them; under either policy one period never produces more
than one execution.

A schedule may be paused and resumed. While paused, due periods create no
executions; on resume the elapsed periods are settled under the declared
misfire policy. Declaring a schedule again replaces the plan (which restarts
from the current moment), validates the new plan before any write, and keeps
the paused/active state.

```http
POST /workflows/order-flow/schedule
Idempotency-Key: schedule-request-1

{"cron":"0 9 * * 1-5","input":{},"misfire_policy":"skip"}

POST /workflows/order-flow/schedule/pause
Idempotency-Key: schedule-pause-1

POST /workflows/order-flow/schedule/resume
Idempotency-Key: schedule-resume-1

GET /workflows/order-flow/schedule
GET /workflows/order-flow/schedule/events
```

Pause and resume carry an empty body. The status query returns the plan as
declared, the `misfire_policy`, whether the schedule is currently `paused`,
the `last_fired_at` time, and the `last_execution_id`; a workflow without a
schedule returns the definite empty result `{"schedule":null}`, while a
missing workflow returns 404. The events endpoint lists, in order, when the
schedule triggered which execution (with the period start), recording only
the schedule's own firings. Pausing or resuming a workflow without a
schedule, or operating on a missing workflow, returns 404 `not_found`;
reusing an idempotency key across different operations returns 409.

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

An execution may also declare its own webhook `subscriptions`, which apply in
addition to the ones its workflow declares:

```json
{
  "id": "run-3",
  "workflow_id": "order-flow",
  "input": {},
  "subscriptions": [{"url": "https://hooks.example.com/ops", "events": ["node_completed"], "max_attempts": 3}]
}
```

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

### Webhook notifications

Subscriptions declared on a workflow or an execution deliver outbound webhook
messages when business events occur. Each subscription contains exactly:

- `url`: a non-empty `http` or `https` address to POST to;
- `events`: a non-empty array of event types without duplicates, drawn from
  `node_completed`, `execution_completed`, `execution_terminated`, and
  `approval_decided` (a termination is the same event type whatever its
  reason);
- `timeout_seconds` (optional): a positive number of seconds to wait for each
  delivery attempt, defaulting to 5;
- `max_attempts` (optional): an integer between 1 and 10, defaulting to 1.

Any other field, a missing `url` or `events`, a mistyped value, an empty or
duplicated event list, an unknown event type, an empty or non-http(s) `url`,
a non-finite number, a non-positive timeout or attempt count, or more than
ten attempts is a 400 `validation_error` and rejects the whole request — no
workflow, execution, or subscription is partially written.

When a subscribed event occurs, a JSON message is POSTed to each matching
target immediately: the body carries `event_type`, `execution_id`, and the
event's details (such as `node_id` for a completed node or `approver` for an
approval decision). Each delivery request carries an `Idempotency-Key`
header; retries of the same event reuse the same key, and different events
never share a key. A delivery is attempted at most the subscription's
`max_attempts` times, with an increasing backoff between attempts. An
unreachable target, a timeout, or a non-2xx response marks the attempt as
failed, but a failed delivery never changes the outcome of the call that
triggered it.

Deliveries do not append execution events, do not add fields to the execution
state, and are never triggered by replay, recovery, or queries, so
checkpoints and replay conclusions are unaffected. An execution that declares
no subscriptions keeps exactly the same state, event stream, and advancement
results as before.

### Inspect the delivery history

```http
GET /executions/run-1/deliveries
```

Returns `{"deliveries": [...]}` in the order the events occurred. Each record
gives its `sequence`, the subscription `url`, the `event_type`, the
`event_sequence` that triggered it, the delivery `idempotency_key`, the final
`status` (`delivered` or `failed`), the `attempt_count`, an `attempts` list
recording each try's `status_code` or `error` (so a retry that eventually
succeeds is visible attempt by attempt), and `occurred_at`. Querying the
history of a missing execution returns 404 `not_found` and records nothing.

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
Executions of workflows that declare approval points additionally expose
`waiting_approval` (`null`, or the pending point's `node_id`, `approvers`, and
loop context) and the `approvals` record list. Each condition evaluation
appends a `condition_evaluated` event and each skip a `node_skipped` event to
the execution stream.

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
`rejected`, `timeout`, or `cancelled` — and appends a single
`execution_terminated` event; completed executions keep a `null` termination
reason and their own `execution_completed` event. Advancing a terminated
execution returns its state unchanged without consuming the submitted output
or failure; a decision against one only succeeds as a repeat of the decision
that rejected it, and otherwise conflicts.

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
10, and a non-positive `timeout_seconds`, are validation errors. A malformed
subscription — a missing or mistyped field, an unknown field, an empty or
duplicated event list, an unknown event type, an empty or non-http(s) `url`,
a non-positive timeout or attempt count, or more than ten attempts — is a
validation error that rejects the whole request without partial writes, and
querying the delivery history of a missing execution is a missing resource. An approval
point with an empty approver list, a duplicate or non-string approver, an
approval on a non-task node, and a decision body that is malformed or carries
a decision other than `approved` or `rejected` are validation errors. An
invalid schedule — a non-positive or non-integer `interval_seconds`, a cron
expression with the wrong field count, out-of-range values or unparseable
fragments, both a plan and a cron, an unknown `misfire_policy`, or an unknown
field — rejects the whole request (workflow creation or schedule declaration)
without partial writes, as does a pause or resume body that carries fields.
Reusing a workflow or execution identifier, or reusing an idempotency key
across different operations, is a conflict. A decision by an approver who is
not listed for the pending point, or any decision against an execution that
has no pending approval point (other than a repeat of the decision that
resolved the latest one), is a conflict. Claiming a work item whose lease is
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

