# ChronicleFlow

ChronicleFlow is a small backend for durable workflow orchestration. It stores
workflow definitions, execution state, and an append-only event history in
SQLite so an execution can be inspected and replayed deterministically.

The initial release intentionally supports a compact public contract:

- workflows are directed acyclic graphs of `task` and `condition` nodes;
- executions advance one ready task at a time;
- ready conditions are evaluated automatically from the execution input;
- tasks guarded by a `run_if` whose condition does not match are skipped;
- every state transition is appended to the execution event stream;
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
    {"id": "is-vip", "kind": "condition", "depends_on": ["reserve"],
     "path": "customer.tier", "equals": "vip"},
    {"id": "gift", "kind": "task", "depends_on": ["is-vip"],
     "run_if": {"condition_id": "is-vip", "expected": true}},
    {"id": "charge", "kind": "task", "depends_on": ["gift"]}
  ]
}
```

Returns HTTP 201 with the stored workflow. Node identifiers must be unique,
dependencies must exist, and cycles are rejected.

#### Condition nodes

A condition node has `kind: "condition"` and two extra fields:

- `path` — a non-empty dot-separated path into the execution input
  (e.g. `customer.tier`);
- `equals` — a JSON scalar (string, number, boolean, or null).

Once its dependencies are resolved, the value at `path` is compared with
`equals` by JSON type and value (`true` is not equal to `1`, `"1"` is not
equal to `1`, `null` only matches an explicit null). A missing path evaluates
to `false`.

#### Task `run_if`

A task may carry `run_if: {"condition_id": "...", "expected": true|false}`.
The referenced node must be a condition and must also appear in the task's
`depends_on`. When the referenced condition has a result different from
`expected`, the task is skipped: it receives no output, but counts as a
resolved dependency for its successors.

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

The first endpoint returns the materialized state: `completed_nodes`
(includes evaluated conditions), `outputs` (tasks only), `condition_results`
(mapping condition id to boolean), and `skipped_nodes`. The second returns
the ordered event stream.

### Complete the next ready task

```http
POST /executions/run-1/advance
Idempotency-Key: advance-request-1

{"output":{"reservation_id":"r-9"}}
```

Each call first evaluates every ready condition (in deterministic order),
skipping any tasks whose `run_if` no longer matches, until no further
conditions or skips are possible. If that processing resolves every node, the
execution becomes `completed` and the request output is **not** consumed.
Otherwise the lexicographically first ready task is completed with the
supplied output. Skipped nodes receive no output but satisfy their
successors' dependencies.

Events appended by conditional execution are `condition_evaluated`
(payload `{"node_id","result"}`) and `node_skipped` (payload
`{"node_id"}`), alongside the existing `node_completed` and
`execution_completed` events.

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

