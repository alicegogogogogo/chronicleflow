# ChronicleFlow

ChronicleFlow is a small backend for durable workflow orchestration. It stores
workflow definitions, execution state, and an append-only event history in
SQLite so an execution can be inspected and replayed deterministically.

The initial release intentionally supports a compact public contract:

- workflows are directed acyclic graphs of `task` and `condition` nodes, with
  optional bounded `loop` nodes wrapping a task/condition sub-DAG;
- executions advance one ready task at a time, evaluating conditions and
  skipping unmatched branches automatically (including inside loop bodies);
- every state transition is appended to the execution event stream;
- loop state (current iteration, per-round node order and outputs, end reason)
  is persisted, so an execution can be resumed or replayed after a restart;
- replay rebuilds execution state from the recorded events;
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

### Loops

A third node kind, `loop`, bounds a recoverable, replayable region of the graph
without introducing a separate protocol — loops are declared through the same
`POST /workflows` node list and driven through the same `advance` endpoint.

```json
{
  "id": "retry",
  "kind": "loop",
  "depends_on": ["prepare"],
  "entry": "attempt",
  "condition_id": "again",
  "max_iterations": 5
}
```

A loop node contains exactly `id`, `kind`, `depends_on`, `entry`,
`condition_id`, and `max_iterations`; any other field is rejected.

- `entry` names a `task` node. The **loop body** is the entry task together
  with every node reachable from it by following dependency edges forward
  (its transitive dependents). No `body` list is declared: membership is
  derived, so an outer node that depends on a body node is simply part of the
  body and cannot be scheduled or written outside a round.
- `condition_id` names a `condition` node that belongs to that body (it is
  reachable from the entry) and therefore observes the round.
- `max_iterations` is an integer between `1` and `1000`.

The body stays a DAG of `task`/`condition` nodes: loops never nest, the entry
cannot reach back to itself, body nodes may only depend on other body nodes
(the entry is thus the round root), a loop may not depend on its own body, and
`run_if` guards may not cross the loop boundary. An empty body, an
out-of-range reference, a non-condition `condition_id`, a zero or out-of-range
`max_iterations`, or a missing idempotency key all yield `400
validation_error`.

A loop only becomes eligible once every node in its `depends_on` has completed
or been skipped — those dependencies are settled before the first round. The
control condition is then judged against the same execution input, using the
same JSON type/value comparison; a missing path is `false`.

- A `false` first judgment completes **zero** rounds with reason
  `condition_false`; the output submitted with that `advance` is not consumed.
- A `true` judgment creates round 1 while the cap is not reached. Within a
  round, conditions auto-evaluate and `run_if`-guarded tasks skip exactly as
  at the top level, and each `advance` completes the lexicographically first
  ready task across the whole execution (body and outer tasks share one global
  ordering). Task outputs belong only to the current round; skipped nodes
  still satisfy their successors.
- When a round is fully completed or skipped, the control condition is judged
  again: `true` starts the next round, `false` ends the loop with
  `condition_false` and releases the loop's successors. When the cap is hit,
  that round is finished first and the loop ends with `iteration_limit`; no
  extra round is created and no extra output is consumed.

The loop node is added to `completed_nodes` (and so unblocks successors) only
after the whole loop finishes; body node ids never appear in the top-level
`completed_nodes` / `skipped_nodes` lists. Execution state gains a `loops` map
keyed by loop node id:

```json
{
  "status": "running",
  "current_iteration": 2,
  "reason": null,
  "iterations": [
    {"iteration": 1, "completed_nodes": ["attempt", "again"],
     "skipped_nodes": [], "condition_results": {"again": true}}
  ]
}
```

`current_iteration` is `0` before the body ever runs, and `reason` is `null`
while running, then `condition_false` or `iteration_limit`. Body task outputs
accumulate under top-level `outputs` as an array ordered by round, and body
conditions store one boolean per evaluation under `condition_results`.

The event stream records the full lifecycle so state is rebuilt from events
alone: `loop_started`, `loop_condition_evaluated` (with `phase` `entry` or
`between`), `iteration_started`, body-level `condition_evaluated`,
`node_completed`, and `node_skipped` events (each tagged with `loop_id` and
`iteration`), `iteration_completed`, and `loop_completed` (carrying `reason`
and the number of iterations). A restart resumes the unfinished round from the
persisted events and state without re-emitting node outputs or completion
events; advancing an already-completed execution returns its stored state and
never absorbs new output.

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

The lexicographically first ready task is completed — across both ordinary
tasks and the current round of any active loop. The response contains the
updated execution. Before that, each call first starts loops whose
prerequisites are settled, evaluates ready conditions, skips tasks whose
`run_if` does not match, and judges a loop's control condition when a round
finishes, all in deterministic order; if this automatic processing finishes
the execution, the current state is returned and the submitted output is not
consumed. When every top-level node (including each finished loop node) is
completed or skipped, the status becomes `completed`.

Execution state includes `completed_nodes` (which also lists evaluated
conditions and finished loop nodes), `skipped_nodes`, `condition_results`,
`outputs`, and `loops`. The top-level lists only contain nodes outside a loop
body; per-round records live under `loops`. Each condition evaluation appends a
`condition_evaluated` event and each skip a `node_skipped` event to the
execution stream. An `advance` against an already `completed` execution
returns its stored state unchanged and absorbs no output.

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
return 409.

## Tests

```bash
PYTHONPATH=src python -m unittest discover -s tests -v
```

