# ChronicleFlow

ChronicleFlow is a small backend for durable workflow orchestration. It stores
workflow definitions, execution state, and an append-only event history in
SQLite so an execution can be inspected and replayed deterministically.

The initial release intentionally supports a compact public contract:

- workflows are directed acyclic graphs of `task`, `condition`, and bounded
  `loop` nodes;
- a workflow may declare named versions: every declared version is kept and
  each execution is bound to the version that was current (or explicitly
  named) when it started, so upgrading a workflow never changes a running
  execution;
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
- workflows and executions may declare webhook subscriptions, and matching
  business events are delivered to the declared targets with bounded retries,
  each delivery recorded in a per-execution history;
- a workflow may declare a schedule — a fixed interval in seconds or a
  five-field cron plan — and the service automatically creates one execution
  per due period with the declared input; schedules can be paused and
  resumed, and periods missed while paused are either caught up once or
  skipped, according to the declared missed policy.

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

### Tenants and quotas

A request may declare a tenant by sending the `X-Tenant-Id` header with a
non-empty identifier. Every workflow, execution, schedule, schedule status,
and delivery history record is scoped to the tenant of the request that
created it: queries and operations only ever see data belonging to that
tenant, and different tenants may use the same workflow or execution
identifiers without colliding. Referencing another tenant's resource — for
example advancing, deciding, claiming, cancelling, recovering, replaying, or
reading the events, checkpoints, deliveries, or schedule of an execution or
workflow owned by another tenant — is answered exactly like a reference to a
missing resource: `404 not_found`, with no indication that the resource
exists elsewhere. An execution created in one tenant can only reference a
workflow of the same tenant; referencing an unseen workflow identifier is the
usual `404 not_found`.

The header applies to every workflow, execution, and schedule entry point,
including creation, advancement, approval decisions, lease operations,
cancellation, recovery, replay, and all history and status queries. An empty
`X-Tenant-Id` value is a `400 validation_error`. Requests that omit the
header entirely keep using the single legacy namespace, whose advancement,
approvals, leases, retries, timeouts, cancellation, checkpoints, recovery,
scheduling, and replay behavior is byte-for-byte unchanged: the tenant is
stored only as database scope, never added to a state field or event payload,
and state shapes and event streams gain no new fields. Idempotency keys are
scoped per tenant as well, so tenants can never see or collide with each
other's keys; reusing a key for another operation within the same tenant is
the usual `409 conflict`.

A tenant may declare quotas bounding how many workflows and executions it may
hold:

```http
PUT /quotas
Idempotency-Key: quota-request-1
X-Tenant-Id: acme

{"workflows": 10, "executions": 100}
```

`POST` to the same path is accepted as well. The body must contain exactly
`workflows` and `executions`, each a positive integer; a non-positive,
non-integer, boolean, or non-finite value, a missing or extra field, or a
non-object body is a `400 validation_error` that writes nothing. Both quota
routes require a tenant: calling them without `X-Tenant-Id` (or with an empty
one) is a `400 validation_error`. The response is
`{"quota":{"workflows":10,"executions":100}}`; declaring again replaces the
limits (re-anchoring nothing else). Lowering a limit below the current
holding deletes no existing data — existing workflows and executions stay
fully usable — it only rejects later writes that would exceed the new limit.

```http
GET /quotas
X-Tenant-Id: acme
```

Returns the declared quota, or the definite empty result `{"quota":null}`
when the tenant has declared none.

When a write would take the tenant past either limit, the whole request is
rejected with `409 conflict` and an error message that names the quota (for
example `quota exceeded: tenant already holds 10 workflows (quota limit is
10)`), so callers can tell quota rejection apart from an ordinary identifier
conflict. The rejection performs no partial write: nothing is inserted and
previously stored data is unaffected, exactly as for validation failures.

A schedule firing on time creates its execution in the schedule's tenant and
that execution counts against the tenant's execution quota. When the tenant
is at its execution limit, the due period creates no execution and the
schedule is left unchanged — its cursor, `last_triggered_at`, and
`last_execution_id` stay as they were — so the period is settled on a later
pass once capacity exists, under the usual per-period idempotence. Delivery
history follows the tenant of the execution it belongs to.

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

A workflow definition may carry an optional `version` tag; declaring one
enables upgrades and keeps every version. Versioning is described under
"Workflow versions" below, and a workflow's stored versions and current
version are available through `GET /workflows/{id}`.

A workflow may also declare webhook `subscriptions` alongside its nodes:

```json
{
  "id": "order-flow",
  "nodes": [{"id": "reserve", "kind": "task", "depends_on": []}],
  "subscriptions": [{"url": "https://hooks.example.com/orders", "events": ["execution_completed"]}]
}
```

See "Webhook notifications" below for the subscription shape and delivery
semantics; the subscriptions apply to every execution of the workflow (for a
versioned workflow, to executions bound to that version).

A workflow may also declare a `schedule` alongside its nodes:

```json
{
  "id": "nightly-orders",
  "nodes": [{"id": "reserve", "kind": "task", "depends_on": []}],
  "schedule": {"interval_seconds": 3600, "input": {"mode": "nightly"}, "missed_policy": "catch_up"}
}
```

See "Schedules" below for the declaration shape and the trigger semantics.

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

### Workflow versions

A workflow can declare a version tag alongside its nodes:

```json
{
  "id": "order-flow",
  "version": "2026-09-25",
  "nodes": [
    {"id": "reserve", "kind": "task", "depends_on": []},
    {"id": "ship", "kind": "task", "depends_on": ["reserve"]}
  ]
}
```

`version` is an optional non-empty string of at most 100 characters, validated
like every other identifier. Posting it when the workflow does not yet exist
creates the workflow with that version; posting another definition with a new
`version` to the same `id` adds a new version without touching the previous
ones — the workflow keeps its entire history, and every stored version remains
usable. The most recently added version becomes the workflow's current
version. Posting a definition without a `version` to an existing workflow
still conflicts, and reusing a version tag already stored for the same
workflow is a `409 conflict`; either rejection writes nothing.

A version declaration may also carry `subscriptions` and a `schedule`,
validated exactly like at creation. Subscriptions are attached to the
declared version and apply only to executions bound to it; a version without
subscriptions replaces none, and other versions' subscriptions stay intact.
Declaring a `schedule` on an added version replaces the workflow's single
schedule plan in the usual way (re-anchored, pause flag kept).

An execution created without an explicit `version` binds to the workflow's
current version at the moment it starts:

```http
POST /executions
Idempotency-Key: execution-versioned

{"id":"run-v2","workflow_id":"order-flow","input":{}}
```

An execution may instead name the version it wants:

```json
{"id":"run-v1","workflow_id":"order-flow","version":"2026-08-01","input":{}}
```

Naming a version that does not exist for the workflow is a missing-resource
`404 not_found`, exactly like naming a workflow that does not exist; the
execution is not created. Everything about an execution follows its bound
version: ready-node selection, condition evaluation, loop rounds, approval
points, retry counts, leases, and the subscriptions that fire for its events.
Adding a new version never interrupts an execution already running: an
execution started before the upgrade keeps advancing on its old version to
completion or termination. Its checkpoints are snapshots of the same state,
recovery rebuilds from the bound version, and replay rebuilds state and the
version binding solely from its event stream; recorded events and historical
conclusions never change.

A versioned execution's state carries its `version` tag, and its
`execution_started` event records the same tag; querying it therefore shows
which version it is bound to. A workflow that never declares a version, and an
execution of one, keep exactly the previous behavior, state shape, and event
stream with no added field.

The definition query lists every version and the current one:

```http
GET /workflows/order-flow
```

```json
{"current_version":"2026-09-25","id":"order-flow","versions":[
  {"id":"order-flow","nodes":[...],"version":"2026-08-01"},
  {"id":"order-flow","nodes":[...],"version":"2026-09-25"}
]}
```

Versions are returned in declaration order; each entry is the stored
definition with its `version` tag. A workflow that never declared a version
returns a single untagged entry and `current_version: null`. A missing
workflow returns 404 `not_found`. Cross-tenant references are missing
resources as usual.

### Start an execution

```http
POST /executions
Idempotency-Key: execution-request-1

{"id":"run-1","workflow_id":"order-flow","input":{"order_id":"o-7"}}
```

Returns HTTP 201. The execution starts in `running` state. Without an
explicit `version`, it binds to the workflow's current version at start time;
naming a `version` binds to that one instead (and a missing version is a
`404 not_found`). See "Workflow versions" for the binding and upgrade rules.

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
Every delivery attempt is recorded, including one whose history record itself
could not be written: such a failure is not swallowed — it is retried once in
a fresh write carrying `status` `failed` and a `persistence_error` describing
the write failure, so the attempt remains visible whenever the database can
accept it (and is logged rather than silently dropped if it cannot).

### Schedules

A workflow may declare a schedule so the service creates executions
automatically. The declaration happens at workflow creation (a `schedule`
field next to `id` and `nodes`) or later through the update endpoint, and it
takes effect from the moment it is stored. A schedule contains exactly:

- `interval_seconds`: a positive integer number of seconds between runs —
  or, alternatively, `cron`: a five-field cron expression (`minute hour
  day-of-month month day-of-week`) whose fields support `*`, `*/step`,
  single values, ranges `a-b`, ranges with steps `a-b/step`, and
  comma-separated lists, with 0 and 7 both meaning Sunday. Exactly one of
  `interval_seconds` and `cron` must be present;
- `input`: the object used as the execution input of every created run;
- `missed_policy`: either `catch_up` or `skip`.

Whenever a period of the plan comes due, the service creates one execution
for the workflow with the declared input. The created execution is exactly a
manually created one: it starts `running` with the same state shape and the
usual `execution_started` event, and advancing, approvals, leases, retries,
timeouts, and cancellation all behave identically. The trigger itself writes
nothing into the created execution's event stream and adds no fields to its
state; the schedule's own record of when it fired and which execution it
created is exposed through the status query below.

Creation is idempotent per schedule period: a repeated trigger or a repeated
request for the same period returns the same execution and never creates a
second one, under either missed policy. When periods were missed — the
schedule was paused, or the service was not running — `catch_up` makes up
exactly the single most recent missed period, while `skip` makes up none;
neither policy ever creates more than one execution for the same period.

An invalid declaration rejects the whole request with 400
`validation_error` and writes nothing: a non-positive or non-integer
`interval_seconds`, a cron expression with other than five fields, out-of
range values, or unparseable fragments, declaring both `interval_seconds`
and `cron`, a `missed_policy` other than `catch_up` or `skip`, a missing or
mistyped `input`, or any unknown field.

### Declare or replace a schedule

```http
PUT /workflows/nightly-orders/schedule
Idempotency-Key: schedule-request-1

{"interval_seconds":3600,"input":{"mode":"nightly"},"missed_policy":"catch_up"}
```

`POST` to the same path is accepted as well. The body is validated exactly
like a schedule declared at creation time, and the response is the schedule
status. Updating replaces the plan and re-anchors it at the update time; the
paused flag is kept. A missing workflow returns 404 `not_found`, an invalid
plan 400 `validation_error` with no partial write, and reusing an
idempotency key from another operation is a 409 `conflict`.

### Inspect the schedule status

```http
GET /workflows/nightly-orders/schedule
```

Returns the declared plan as given (`interval_seconds` or `cron`, the
`input`, and the `missed_policy`), whether the schedule is currently
`paused`, the `last_triggered_at` time, and the `last_execution_id` of the
most recently created execution (both `null` until the first trigger). A
workflow that never declared a schedule returns the definite empty result
`{"schedule":null}`, and a missing workflow returns 404 `not_found`.

### Pause and resume a schedule

```http
POST /workflows/nightly-orders/schedule/pause
Idempotency-Key: pause-request-1

{}
```

```http
POST /workflows/nightly-orders/schedule/resume
Idempotency-Key: resume-request-1

{}
```

Both bodies must be empty objects; a missing or mistyped body, an unknown
field, or a non-finite number is a 400 `validation_error`. While paused, due
periods create no executions. Resuming settles the periods that came due
during the pause according to the missed policy: `catch_up` immediately
creates the single most recent missed run, `skip` creates none. Pausing or
resuming a workflow that has no schedule, or any schedule operation on a
missing workflow, returns 404 `not_found`; reusing an idempotency key across
operations returns 409 `conflict`. Both responses carry the schedule status.

Workflows that declare no schedule are unaffected: their creation,
advancement, approvals, deliveries, checkpoints, recovery, and replay behave
exactly as before, with no additional fields or events.

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
return 409. An empty `X-Tenant-Id` header value is a validation error; quota
declarations require a tenant and positive integer limits, validated by the
same rules as every other body (no non-finite numbers, no unknown fields).
Reusing a workflow or execution identifier, adding a workflow version whose
tag already exists for that workflow, or reusing an idempotency key across
different operations (within the same tenant), is a conflict. Starting an
execution against a missing workflow or a missing workflow version is a
missing resource (`404 not_found`).
Exceeding a tenant's declared workflow or execution quota is also a `409
conflict`, with the word "quota" in the message so it can be distinguished
from an identifier conflict; the rejected request writes nothing. Referencing
a resource owned by another tenant is a missing resource (`404
not_found`), indistinguishable from one that does not exist, so existence is
never revealed across tenants. A `retries` value that is negative, non-integer, or greater than
10, and a non-positive `timeout_seconds`, are validation errors. A malformed
subscription — a missing or mistyped field, an unknown field, an empty or
duplicated event list, an unknown event type, an empty or non-http(s) `url`,
a non-positive timeout or attempt count, or more than ten attempts — is a
validation error that rejects the whole request without partial writes, and
querying the delivery history of a missing execution is a missing resource. An approval
point with an empty approver list, a duplicate or non-string approver, an
approval on a non-task node, and a decision body that is malformed or carries
a decision other than `approved` or `rejected` are validation errors.
A decision by an approver who is
not listed for the pending point, or any decision against an execution that
has no pending approval point (other than a repeat of the decision that
resolved the latest one), is a conflict. Claiming a work item whose lease is
still active, submitting results for a work item held by another worker or
after the lease expired, and heartbeating or releasing a lease held by
another worker are conflicts, while heartbeating or releasing an execution
with no claimed work item is a missing resource. Recovering a missing
execution is a missing resource, while recovering an execution that has no
checkpoint or whose latest checkpoint is unparseable is a conflict; an
invalid recover body is a validation error. An invalid schedule declaration —
a non-positive or non-integer interval, a malformed or out-of-range cron
expression, declaring both an interval and a cron plan, an unknown missed
policy, or an unknown field — is a validation error that rejects the whole
request without partial writes; pausing, resuming, or updating the schedule
of a missing workflow, or pausing and resuming a workflow that has no
schedule, is a missing resource, and a malformed pause or resume body is a
validation error. Request bodies must not contain
non-finite numbers (`NaN`, `Infinity`, or overflowing values such as `1e400`);
they are rejected with 400. Finite floats keep their full precision, including negative zero
(`-0.0`), and every response body ends with a single newline.

## Tests

```bash
PYTHONPATH=src python -m unittest discover -s tests -v
```

