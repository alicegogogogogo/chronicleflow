# ChronicleFlow

ChronicleFlow is a small backend for durable workflow orchestration. It stores
workflow definitions, execution state, and an append-only event history in
SQLite so an execution can be inspected and replayed deterministically.

The initial release intentionally supports a compact public contract:

- workflows are directed acyclic graphs of `task`, `condition`, bounded
  `loop`, and dynamic `map` nodes;
- a workflow may declare named versions: every declared version is kept and
  each execution is bound to the version that was current (or explicitly
  named) when it started, so upgrading a workflow never changes a running
  execution, and a running execution can be explicitly migrated to another
  version of the same workflow, after which it advances under that version;
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
- a map node inside a loop body expands independently in every iteration
  from that iteration's source output, its instances and events belonging
  to the iteration they expanded in;
- every state transition is appended to the execution event stream;
- replay rebuilds execution state from the recorded events;
- a checkpoint is written at every node boundary, capturing the state
  summary and event position, so an execution can be recovered from the
  latest checkpoint after a restart;
- workers may claim a running execution to receive a work item with a
  time-bounded lease, renew the lease with heartbeats, and release it;
  while a lease is active only its holder may submit results, and an
  expired or released lease returns the work item to the claimable set;
  a target-scoped claim instead leases one independent ready target, so
  several workers can settle different ready targets of one execution in
  parallel;
- duplicate commands with the same idempotency key return the original result;
- workflows and executions may declare webhook subscriptions, and matching
  business events are delivered to the declared targets with bounded retries,
  each delivery recorded in a per-execution history;
- subscriptions may also declare queue targets instead of webhook addresses:
  matching business events are appended, in occurrence order, to persistent
  per-execution queues that callers pull from and acknowledge explicitly,
  with a visibility timeout returning unacknowledged messages to the pending
  set (delivery is at least once);
- a workflow may declare a schedule — a fixed interval in seconds or a
  five-field cron plan — and the service automatically creates one execution
  per due period with the declared input; schedules can be paused and
  resumed, periods missed while paused are either caught up once or
  skipped, according to the declared missed policy, and a read-only preview
  query projects the upcoming trigger times and their inputs.

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

Responses are compact JSON with no insignificant whitespace, and every
response body ends with exactly one trailing newline (`\n`). Numbers
round-trip with their full precision: finite floats keep their original
IEEE-754 value through the shortest textual representation that decodes to
it (so `0.30000000000000004` stays `0.30000000000000004`), negative zero is
preserved as `-0.0`, and integers stay integers. Non-finite numbers may
never appear in a request and are never produced in a response; timestamps
are ISO-8601 UTC strings ending in `Z`.

### Tenants and quotas

A request may declare a tenant by sending the `X-Tenant-Id` header with a
non-empty identifier. Every workflow, execution, schedule, schedule status,
delivery history, and queue record is scoped to the tenant of the request that
created it: queries and operations only ever see data belonging to that
tenant, and different tenants may use the same workflow or execution
identifiers without colliding. Referencing another tenant's resource — for
example advancing, migrating, deciding, claiming, cancelling, recovering, replaying, or
reading the events, checkpoints, deliveries, queues, or schedule of an execution or
workflow owned by another tenant, or pulling from or acknowledging one of its
queues — is answered exactly like a reference to a
missing resource: `404 not_found`, with no indication that the resource
exists elsewhere. An execution created in one tenant can only reference a
workflow of the same tenant, and a migration can only name a version of that
workflow in the same tenant; referencing an unseen workflow identifier or
version is the usual `404 not_found`.

The header applies to every workflow, execution, and schedule entry point,
including creation, advancement, migration, approval decisions, lease
operations, cancellation, recovery, replay, instance deletion and
modification and re-expansion, queue pulls, acknowledgements, and all
history and status queries (the schedule change history included), as well as the quota declaration and delete, the quota
read and remaining query, the quota change history, the price
declaration and price delete, the price read and the price change history, and the usage, bill, and
metrics queries. An empty
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
`workflows` and `executions`, each a positive integer, and may additionally
carry the optional `growth` object described below; a non-positive,
non-integer, boolean, or non-finite value, a missing or extra field, or a
non-object body is a `400 validation_error` that writes nothing. Every quota
route requires a tenant: declaring, deleting, and reading without
`X-Tenant-Id` (or with an empty one) is a `400 validation_error`, and an
unknown query parameter on the delete or the remaining query is rejected the
same way, with the message naming the offending parameter. The response is
`{"quota":{"workflows":10,"executions":100}}`; declaring again replaces the
limits (re-anchoring nothing else). Lowering a limit below the current
holding deletes no existing data — existing workflows and executions stay
fully usable — it only rejects later writes that would exceed the new limit.

Besides the two limits a declaration may carry one optional `growth` object
naming an automatic expansion policy for each limited resource. A policy
names only the resources it covers, and a resource it omits is never raised
automatically. Each named resource must declare exactly a positive integer
`step` and a positive integer `cap`, and the cap must not be below the
resource's declared limit:

```http
PUT /quotas
Idempotency-Key: quota-request-2
X-Tenant-Id: acme

{"workflows":10,"executions":100,"growth":{"workflows":{"step":5,"cap":50},"executions":{"step":50,"cap":500}}}
```

A missing field, an unknown field (inside the policy or beside it), a
non-object policy, a resource other than `workflows` or `executions`, or a
step or cap that is non-positive, non-integer, boolean, or non-finite is a
`400 validation_error`; a cap below the resource's declared limit is rejected
the same way. Every such rejection writes nothing. A declaration without
`growth` is byte-for-byte the two-limit request above; repeating a declaration
replaces limits and policy as a whole, so a declaration that names no `growth`
also drops the tenant's previous policy. The declaration response and the
quota read include the policy only when one is declared, with each entry
listing `step` before `cap` and resources in workflows-first order:

```json
{"quota":{"workflows":10,"executions":100,"growth":{"workflows":{"step":5,"cap":50},"executions":{"step":50,"cap":500}}}}
```

```http
GET /quotas
X-Tenant-Id: acme
```

Returns the declared quota, or the definite empty result `{"quota":null}`
when the tenant has declared none.

A tenant can also remove the quota it declared, on the same path with the
`DELETE` method and the same calling shape as the other quota writes:

```http
DELETE /quotas
Idempotency-Key: quota-delete-1
X-Tenant-Id: acme

{}
```

The body must be exactly an empty object; a non-empty or non-object body, an
unknown field, or a non-finite value is a `400 validation_error` that touches
no quota. The delete removes the declaration as a whole and leaves no partial
write, so a later read gives the same definite empty result as for a tenant
that never declared one: the delete response and every following read both
return `{"quota":null}`, and the remaining query lists neither resource —
neither limit, holding, nor remaining — and repeats with exactly the same
result. Deleting the quota of a tenant that never declared one returns that
same definite empty result rather than an error. The delete clears only the
declaration: it deletes no existing workflow or execution, and data already
held stays fully usable. It writes no metering record, so recorded usage
counts and bill conclusions already reached stay exactly as they were; later
writes simply run without a quota check until the tenant declares again.

The delete follows the same idempotency rules as every other write: the
`Idempotency-Key` header is required, repeating the same delete with the same
key returns the first result and performs no second removal, and reusing a
key for another operation — including a quota declaration — in the same
tenant is a `409 conflict` that leaves the tenant's quota unchanged. Quotas
are scoped per tenant like everything else: deleting one tenant's quota never
affects another tenant's declaration or remaining room. A missing or empty
tenant and a missing idempotency key are `400 validation_error`, and every
failed validation writes nothing — no quota row, no metering record, and no
quota or price-table change history.

A separate read-only entry point reports how much room the declared quota
leaves, with the same calling shape as the quota read:

```http
GET /quotas/status
X-Tenant-Id: acme
```

Each limited resource gives three numbers — the declared `limit`, the
`held` count the quota decision itself uses, and the `remaining` room, which
is simply limit minus holding — in that key order, with workflows first and
executions second; the same query repeats with the same ordering:

```json
{"status":{"workflows":{"limit":10,"held":3,"remaining":7},"executions":{"limit":100,"held":12,"remaining":88}}}
```

Lowering a limit below the current holding reports the `remaining` room
truthfully as a negative number; it is neither clamped to zero nor rejected.
A tenant that has never declared a quota gets the definite empty result
`{"status":null}`, listing neither resource. The query is strictly read-only:
it writes no metering record and changes no declared quota, usage, bill,
scheduling, approval, or replay conclusion, and another tenant's quota and
holdings are never visible and never influence the result. It requires a
tenant and accepts no query parameters: a missing or empty `X-Tenant-Id`
header, an unknown parameter (named in the error message), or a repeated
parameter is a `400 validation_error`, and these rejections write nothing.

When a write would take the tenant past either limit, the write is admitted
once the resource declares a growth policy that can make room: the declared
limit is raised in whole steps — the smallest number of `step` increments
that admits the write — never past the `cap` and never by a partial final
step, and the raised limit commits with the write in the same transaction,
so the request succeeds instead of returning a conflict. The expansion
changes only the declared limit: it writes no metering record, changes no
recorded usage count, and changes no bill conclusion already reached.
Repeating the same write with the same idempotency key returns the first
result without a second expansion, and reusing that key for another
operation is the usual `409 conflict`. Without a policy on the resource, or
when no whole-step raise within the cap can admit the write — including when
the cap already leaves no room for one more holding — the whole request is
rejected with `409 conflict` and an error message that names the quota (for
example `quota exceeded: tenant already holds 10 workflows (quota limit is
10)`), so callers can tell quota rejection apart from an ordinary identifier
conflict. The rejection performs no partial write and leaves the limit
untouched: nothing is inserted and previously stored data is unaffected,
exactly as for validation failures. The quota read and the remaining query
report the raised limit; the remaining query's `remaining` room is computed
from it.

A schedule firing on time creates its execution in the schedule's tenant and
that execution counts against the tenant's execution quota. When the tenant
is at its execution limit, a growth policy raises the limit exactly as for a
manual creation and the period fires normally; when there is no policy or
the cap leaves no room a whole step can reach, the due period creates no
execution and the schedule is left unchanged — its cursor,
`last_triggered_at`, and `last_execution_id` stay as they were — so the
period is settled on a later pass once capacity exists, under the usual
per-period idempotence. Delivery history follows the tenant of the execution
it belongs to.

### Quota change history

Every successful quota declaration or delete leaves one change record, so a
caller can see exactly when each quota declaration and each removal took
effect. A separate read-only entry point returns the tenant's change history
with the same calling shape as the price-table change history:

```http
GET /quotas/history?limit=50
X-Tenant-Id: acme
```

Returns `{"history":[...]}` with the records in ascending order of occurrence
time; records sharing one instant are ordered by a stable ascending sequence.
Each record is:

```json
{"sequence":7,"action":"declare","occurred_at":"2026-09-26T08:30:00.000Z","snapshot":{"workflows":10,"executions":100,"growth":{"workflows":{"step":5,"cap":50},"executions":{"step":50,"cap":500}}}}
```

- `sequence` is a stable, strictly increasing positive integer per tenant,
  shared across both kinds of quota change. It identifies the record across
  every query and is never renumbered by a filter or a page;
- `action` is `declare` for a quota declaration or `delete` for a removal;
- `occurred_at` is the moment the change took effect, an ISO-8601 UTC string
  ending in `Z`;
- `snapshot` is the complete quota in effect after the change. A
  declaration's snapshot is the limits and growth policy that took effect:
  the two limits first, `workflows` before `executions`, followed by the
  optional `growth` object containing only the resources this declaration
  named, each entry listing `step` before `cap`; a declaration without a
  growth policy lists no `growth` at all. A delete's snapshot is `null`.

The history record is written atomically with the change it describes, in the
same transaction as the quota write, so a reader always sees the declaration
and its history agree and a declaration or delete rejected by validation, a
quota conflict, or an idempotency-key conflict leaves no record. Repeating an
idempotent declaration or delete with the same key returns the first result
and appends no second record; reusing a key for another operation is the
usual `409 conflict` and, again, writes no history. Deleting the quota of a
tenant that never declared one still succeeds and still leaves its own
`delete` record (with a `null` snapshot), exactly as it returns the definite
empty result for the quota itself.

A tenant with no matching records gets the definite empty result
`{"history":[]}`, never an error. The response is one line of compact JSON
with the record keys in the order above, full number precision preserved, and
a single trailing newline, like every other JSON endpoint.

The endpoint accepts an optional action filter, the same optional closed time
window the per-record usage query accepts, plus cursor pagination, in exactly
the same shape as the price-table change history:

```http
GET /quotas/history?action=declare,delete&since=2026-09-26T08:00:00.000Z&until=2026-09-26T09:00:00.000Z&cursor=12&limit=50
```

- `action` names one action or a comma-separated set of them (`declare` and
  `delete`); only records whose action is in the set are returned. A single
  value needs no comma. An empty entry, a duplicate entry, or an unknown
  action is a `400 validation_error`; when `action` is absent every change is
  returned;
- `since` and `until` are ISO-8601 UTC timestamps ending in `Z` and bind a
  **closed** interval on each record's occurrence time: a record whose time
  equals either boundary is included. When either is absent the corresponding
  bound is open; when `since` is later than `until` the window matches nothing
  and the definite empty list is returned;
- `limit` is **required** and must be a positive integer; a page never
  contains more than that many records;
- `cursor` is the `sequence` of the previous page's last record; only records
  whose `sequence` is strictly greater are returned, so pages neither overlap
  nor skip. It is omitted on the first page and must be a positive integer;
- the action filter, time window, and pagination apply together as an
  intersection, and filtering never changes a record's `sequence`.

A malformed timestamp, an unknown, empty, or duplicated `action` entry, a
missing or non-positive-integer `limit`, a non-positive-integer `cursor`, a
repeated parameter, or any unknown query parameter is a `400
validation_error` that writes nothing. A missing or empty `X-Tenant-Id` is a
`400 validation_error` that reveals no records, and another tenant's history
is never visible under any filter. The query is read-only: it appends no
history, writes no metering record, and changes no quota declaration, usage,
bill, scheduling, approval, or replay conclusion. A quota declaration or
delete not participating in this change — reads, the remaining query,
scheduling, metering, and billing included — is byte-for-byte unchanged, and
no metering record is added. After a replay or a recovery each record's
sequence, occurrence time, and snapshot stay exactly as they were first
written.

### Tenant price tables

The bill's baseline unit prices are built in. A tenant may instead declare
its own price table on a separate entry point with the same calling shape as
the quota declaration:

```http
PUT /prices
Idempotency-Key: price-request-1
X-Tenant-Id: acme

{"execution_started":200,"workflow_created":1500}
```

`POST` to the same path is accepted as well. The body must be a non-empty
JSON object giving a map of metered action type to a positive-integer unit
price in cents. A non-positive, non-integer, boolean, or non-finite price,
an unknown metered type, a missing field (an empty object), an extra field
that is not a metered type, or a non-object body is a `400
validation_error` that writes nothing. Every price route requires a tenant:
declaring, deleting, and reading without `X-Tenant-Id` (or with an empty
one) is a `400 validation_error`, and an unknown query parameter on the
read or the delete is rejected the same way, with the message naming the
offending parameter. The response is
`{"prices":{"execution_started":200,"workflow_created":1500}}`, with the
declared types in ascending type-identifier order.

A successful declaration replaces the tenant's price table as a whole with
exactly the types given this time; metered types the declaration does not
name fall back to the built-in default prices when a bill is taken. The
declaration rewrites no recorded usage counts and changes no bill taken
before it was made.

```http
GET /prices
X-Tenant-Id: acme
```

Returns the price table currently declared by the tenant, in ascending
type-identifier order, or the definite empty result `{"prices":null}` for a
tenant that has never declared one. The stored table lists only the types
the declaration named; types left at the default are not copied into it.

A tenant can also remove the table it declared, on the same path with the
`DELETE` method and the same calling shape as the other price writes:

```http
DELETE /prices
Idempotency-Key: price-delete-1
X-Tenant-Id: acme

{}
```

The body must be exactly an empty object; a non-empty or non-object body,
an unknown field, or a non-finite value is a `400 validation_error` that
touches no price table. The delete removes the table as a whole and leaves
no partial write: it is atomic with the declaration and the read, so a
concurrent reader sees either the complete old table or no table at all.
After it succeeds the tenant no longer holds a declared table, and both the
delete response and the following read give the definite empty result
`{"prices":null}`. Deleting the table of a tenant that never declared one
returns that same definite empty result rather than an error. The delete
rewrites no recorded usage counts and changes no bill taken before it was
made; afterward both the bill and the bucketed bill price every type at the
built-in default, each line still giving the metered type, the actual
`unit_price` used, the `count`, and the integer-cent `subtotal`.

The declaration and the delete follow the same idempotency rules as every
other write: the `Idempotency-Key` header is required, repeating the same
declaration or the same delete with the same key returns the first result
and performs no second replacement or removal, and reusing a key for
another operation — including another price declaration or a price delete
— in the same tenant is a `409 conflict` that leaves the tenant's price
table unchanged. Price tables are scoped per tenant like everything else: a
tenant can never read or delete another's declaration, and another tenant's
declaration or deletion never affects this tenant's bill amounts.

The bill and the bucketed bill price with the price table in effect **at
query time**: a named type uses the tenant's declared price and every other
type uses the built-in default. Each returned line still gives the metered
type, the actual `unit_price` used, the `count`, and the integer-cent
`subtotal`, and the total is exactly the sum of the listed subtotals; types
with no matching record are still omitted rather than listed at zero. A
declaration affects only later queries — it neither rewrites recorded usage
nor revises historical bills — changes no metering, and leaves the bill and
usage queries read-only. A delete affects only later queries in the same
way: the table is removed as a whole, later bills fall back to the
built-in defaults, and recorded usage and already-taken bills stay exactly
as they were.

### Price-table change history

Every successful price-table declaration or delete leaves one change record,
so a caller can see exactly when each repricing and each table removal took
effect. A separate entry point returns the tenant's change history:

```http
GET /prices/history?limit=50
X-Tenant-Id: acme
```

Returns `{"history":[...]}` with the records in ascending order of occurrence
time; records sharing one instant are ordered by a stable ascending sequence.
Each record is:

```json
{"sequence":7,"action":"declare","occurred_at":"2026-09-26T08:30:00.000Z","snapshot":{"execution_started":200}}
```

- `sequence` is a stable, strictly increasing positive integer per tenant,
  shared across every kind of price change. It identifies the record across
  every query and is never renumbered by a filter or a page;
- `action` is `declare` for a price-table declaration or `delete` for a
  removal;
- `occurred_at` is the moment the change took effect, an ISO-8601 UTC string
  ending in `Z`;
- `snapshot` is the table in effect after the change. A declaration's
  snapshot is the type-to-unit-price map that took effect, listing only the
  types that declaration named and in ascending type-identifier order; a
  delete's snapshot is `null`.

The history record is written atomically with the change it describes, in the
same transaction as the price-table write, so a reader always sees the table
and its history agree and a rejected declaration, delete, or idempotency-key
conflict leaves no record. Repeating an idempotent declaration or delete with
the same key returns the first result and appends no second record; reusing a
key for another operation is the usual `409 conflict` and, again, writes no
history. A delete of a table that was never declared still succeeds and still
leaves its own `delete` record (with a `null` snapshot), exactly as it returns
the definite empty result for the table itself.

A tenant with no matching records gets the definite empty result
`{"history":[]}`, never an error. The response is one line of compact JSON
with the record keys in the order above, full number precision preserved, and
a single trailing newline, like every other JSON endpoint.

The endpoint accepts an optional action filter, the same optional closed time
window the per-record usage query accepts, plus cursor pagination:

```http
GET /prices/history?action=declare,delete&since=2026-09-26T08:00:00.000Z&until=2026-09-26T09:00:00.000Z&cursor=12&limit=50
```

- `action` names one action or a comma-separated set of them (`declare` and
  `delete`); only records whose action is in the set are returned. A single
  value needs no comma. An empty entry, a duplicate entry, or an unknown
  action is a `400 validation_error`; when `action` is absent every change is
  returned;
- `since` and `until` are ISO-8601 UTC timestamps ending in `Z` and bind a
  **closed** interval on each record's occurrence time: a record whose time
  equals either boundary is included. When either is absent the corresponding
  bound is open; when `since` is later than `until` the window matches nothing
  and the definite empty list is returned;
- `limit` is **required** and must be a positive integer; a page never
  contains more than that many records;
- `cursor` is the `sequence` of the previous page's last record; only records
  whose `sequence` is strictly greater are returned, so pages neither overlap
  nor skip. It is omitted on the first page and must be a positive integer;
- the action filter, time window, and pagination apply together as an
  intersection, and filtering never changes a record's `sequence`.

A malformed timestamp, an unknown, empty, or duplicated `action` entry, a
missing or non-positive-integer `limit`, a non-positive-integer `cursor`, a
repeated parameter, or any unknown query parameter is a `400
validation_error` that writes nothing. A missing or empty `X-Tenant-Id` is a
`400 validation_error` that reveals no records. The query is read-only: it
appends no history, changes no price table, and changes no metering, usage,
bill, quota, or scheduling conclusion. History follows the usual tenant
isolation, so another tenant's changes are never visible under any action
filter, window, or page and never affect this tenant's result; replay and
recovery leave every recorded sequence, time, and snapshot unchanged.

### Usage metering and billing

Requests that carry a tenant identifier leave a usage record for each billable
action they perform. Only tenant-scoped requests are metered: a request that
omits the `X-Tenant-Id` header keeps the legacy namespace and writes no usage
record at all, with its state shapes and event content unchanged. The four
metered action types are:

- `workflow_created` — a workflow definition is stored. Adding a new version
  to an existing workflow is one workflow creation, the same as the first
  definition;
- `execution_started` — an execution is created and starts `running`;
- `schedule_triggered` — a due schedule period creates its execution. The
  scheduled execution is metered both as `execution_started` and as
  `schedule_triggered`;
- `delivery_attempted` — one outbound webhook HTTP attempt is made, or one
  queue message is delivered to a caller pulling from a queue. A
  subscription that retries is metered once per attempt, so a delivery that
  fails and then succeeds on its second attempt records two attempts; the
  action is metered whether it ultimately succeeds or fails. A queue pull
  meters one attempt per message it delivers, so a redelivery after the
  visibility timeout is metered again, while a pull that delivers nothing
  meters nothing.

Metering is atomic with the action it describes: a write rejected by
validation or by a quota produces no record and performs no partial write.
Repeating an idempotent command returns its first result and writes no second
record, and settling the same schedule period more than once (a repeated
trigger or a later pass) meters that period only once. Replay, recovery, and
queries never meter and never alter recorded usage or billing conclusions.

```http
GET /usage
X-Tenant-Id: acme
```

Returns `{"usage":[{"type":"execution_started","count":1}, ...]}`: the
cumulative count of recorded actions per type, sorted by ascending type
identifier. Types with no records are omitted, so a tenant with no usage gets
the definite empty result `{"usage":[]}`.

```http
GET /bill
X-Tenant-Id: acme
```

Returns the per-type bill:

```json
{"bill":{"items":[
  {"type":"execution_started","count":1,"unit_price":100,"subtotal":100},
  {"type":"workflow_created","count":2,"unit_price":1000,"subtotal":2000}
],"total":2100}}
```

Each item gives the metered `count`, a positive-integer `unit_price` in cents
that is also returned in the response, and a `subtotal` equal to
`count * unit_price`; `total` is the integer-cent sum of every subtotal. With
no records, `items` is empty and `total` is `0`. The price used per type is
the tenant's declared price when its price table names that type and the
built-in default otherwise, read from the table in effect at query time (see
[Tenant price tables](#tenant-price-tables)); tenants with no declaration are
billed byte for byte as before.

Both endpoints accept the same optional query parameters the events and
metrics queries accept: an optional metered-type filter and an ISO-8601 UTC
timestamp window, each timestamp ending in `Z`, plus an optional fixed time
bucket:

```http
GET /usage?type=execution_started,workflow_created&since=2026-09-26T08:00:00.000Z&until=2026-09-26T09:00:00.000Z
GET /bill?type=execution_started&since=2026-09-26T08:00:00.000Z&until=2026-09-26T09:00:00.000Z
X-Tenant-Id: acme
```

- `type` names one metered action type or a comma-separated set of them; only
  records whose type is in the set are counted. A single identifier needs no
  comma. An empty entry, a duplicate entry, or an unknown metered type is a
  `400 validation_error`; when `type` is absent every type is counted, exactly
  as before;
- `since` and `until` bound a **closed** interval on each usage record's
  occurrence time: a record whose time equals `since` or `until` is counted.
  When either parameter is absent the corresponding bound is open, so omitting
  both returns exactly the cumulative result above;
- a malformed `since` or `until`, a repeated parameter, or any unknown query
  parameter is a `400 validation_error` that writes nothing;
- when `since` is later than `until` the request is not an error: the window
  simply contains no records. Usage then reports the definite empty result
  `{"usage":[]}`, and the bill reports an empty `items` list with a `total` of
  `0`.

The type filter and the time window apply together as an intersection: a
record is counted only when its type is named **and** its occurrence time is
in the window. A type the filter names but that has no matching record is
omitted rather than reported at zero, so a filter that matches nothing gives
the same definite empty results as an empty window: `{"usage":[]}` for usage
and an empty `items` list with a `total` of `0` for the bill.

Both endpoints also accept an optional `bucket` parameter for a fixed-bucket
trend summary, so callers never have to pull records and group them
themselves:

```http
GET /usage?bucket=hour&since=2026-09-26T00:00:00.000Z&until=2026-09-27T00:00:00.000Z
GET /bill?bucket=day&type=execution_started
X-Tenant-Id: acme
```

- `bucket` is exactly `hour` or `day`: `hour` buckets start on the UTC hour
  and `day` buckets at UTC midnight, both aligned solely to UTC. An empty or
  any other value is a `400 validation_error`, exactly like a repeated
  parameter or an unknown query parameter;
- when `bucket` is present, usage returns
  `{"usage_buckets":[{"bucket_start":"2026-09-26T08:00:00Z","usage":[{"type":"execution_started","count":1}, ...]}, ...]}`:
  the buckets with at least one matching record, in ascending bucket-start
  order. Each entry gives its `bucket_start` first — an ISO-8601 UTC string
  ending in `Z` at the hour or midnight — followed by `usage`, the per-type
  counts inside that bucket in ascending type-identifier order; a type with no
  record in the bucket is omitted;
- when `bucket` is present, the bill returns
  `{"bill":{"buckets":[{"bucket_start":"2026-09-26T00:00:00Z","items":[{"type":"execution_started","count":1,"unit_price":100,"subtotal":100}, ...],"subtotal":100}, ...],"total":100}}`.
  Buckets appear in ascending bucket-start order; each bucket's `items` stay
  sorted by ascending type identifier, each line giving the filtered count,
  the usual positive-integer `unit_price`, and an integer-cent `subtotal` of
  the two, and a type with no matching record is omitted rather than reported
  at zero. Each bucket also carries its own `subtotal`, and the window
  `total` is exactly the sum of the listed per-bucket subtotals, all amounts
  integer cents;
- a bucket that holds no matching record never appears, so an empty tenant, a
  window with `since` later than `until`, or a filter that matches nothing
  returns the definite empty bucket lists `{"usage_buckets":[]}` and
  `{"bill":{"buckets":[],"total":0}` — never an error, with the empty bill's
  total at `0`;
- bucketing applies together with the type filter and the time window as an
  intersection: a bucket holds only records whose occurrence time is in the
  closed window **and** whose type the filter names. The window keeps its
  closed endpoints, its open bounds when either side is omitted, and the
  existing rule for when a record's instant falls in it; buckets are simply
  the same matching records rolled up to their UTC bucket;
- the bucketed queries are read-only like the others: they append no usage
  records, change no metering basis or prices, and never alter recorded usage
  or billing conclusions — under event replay or recovery the same window,
  type filter, and bucket always give the same answer;
- omitting `bucket` leaves both queries exactly as before: the same compact
  JSON responses with `usage` / `items` shapes, full float precision and the
  `-0.0` convention, byte for byte. The parameter introduces no second
  protocol and changes no entry point's existing behavior.

Only records whose occurrence time falls in the window are counted. Within
the window the bill keeps the same basis as always: each present type reports
its filtered, windowed count, its usual `unit_price`, and a `subtotal` of the
two; items stay sorted by ascending type identifier, and a type with no
matching record is omitted rather than reported at zero. The bill's `total`
is exactly the sum of the subtotals of the listed items. The query is
read-only: it writes no usage records, changes no prices or metering
conclusions, and never alters already recorded usage — under event replay or
recovery the same window and type filter always give the same answer.

Both endpoints are tenant-scoped `GET` requests: a missing or empty
`X-Tenant-Id` is a `400 validation_error` that reveals no usage. Usage and
bill data follow the usual tenant isolation, so one tenant can never see
another's records under any type filter or window, and another tenant's
records never affect this tenant's counts or totals.

### Per-record usage history

The cumulative usage query reports only per-type totals. A separate endpoint
returns the individual usage records themselves, so each billed action can be
checked against the moment it occurred:

```http
GET /usage/records?limit=50
X-Tenant-Id: acme
```

Returns `{"records":[...]}` with the matching records in ascending order of
occurrence time; records sharing one instant are ordered by ascending
sequence. Each record is:

```json
{"sequence":7,"type":"execution_started","occurred_at":"2026-09-26T08:30:00.000Z"}
```

- `sequence` is a stable, strictly increasing positive integer per tenant. It
  identifies the record across every query and is never renumbered by a
  filter or a page;
- `type` is the metered action type;
- `occurred_at` is the record's occurrence time as an ISO-8601 UTC string
  ending in `Z`.

A tenant with no matching records gets the definite empty result
`{"records":[]}`, never an error. The response ends with a single newline,
like every other JSON endpoint.

The endpoint accepts an optional metered-type filter, the same optional
closed time window the events and usage queries accept, plus cursor
pagination:

```http
GET /usage/records?type=execution_started,workflow_created&since=2026-09-26T08:00:00.000Z&until=2026-09-26T09:00:00.000Z&cursor=12&limit=50
```

- `type` names one metered action type or a comma-separated set of them;
  only records whose type is in the set are returned. A single identifier
  needs no comma. An empty entry, a duplicate entry, or an unknown metered
  type is a `400 validation_error`; when `type` is absent every type is
  returned, exactly as before;
- `since` and `until` are ISO-8601 UTC timestamps ending in `Z` and bind a
  **closed** interval on each record's occurrence time: a record whose time
  equals either boundary is included. When either is absent the corresponding
  bound is open; when `since` is later than `until` the window matches
  nothing and the definite empty list is returned;
- `limit` is **required** and must be a positive integer; a page never
  contains more than that many records;
- `cursor` is the `sequence` of the previous page's last record; only records
  whose `sequence` is strictly greater are returned, so pages neither overlap
  nor skip. It is omitted on the first page and must be a positive integer;
- the type filter, time window, and pagination apply together as an
  intersection, and filtering never changes a record's `sequence`.

A malformed timestamp, an unknown, empty, or duplicated `type` entry, a
missing or non-positive-integer `limit`, a non-positive-integer `cursor`, a
repeated parameter, or any unknown query parameter is a `400
validation_error` that writes nothing. A missing or empty `X-Tenant-Id` is a
`400 validation_error` that reveals no records. The query is read-only: it
appends no usage records, writes no events, and changes no metering, prices,
quotas, or billing conclusions. Records follow the usual tenant isolation, so
another tenant's records are never visible under any type filter, window, or
page and never affect this tenant's result; replay and recovery leave every
recorded sequence, time, and billing conclusion unchanged.

### Operational metrics

```http
GET /metrics
X-Tenant-Id: acme
```

Returns one line of compact JSON summarizing the business facts already
recorded for the tenant, ending with the usual single newline. The endpoint
is read-only: it appends no events, writes no usage records, and changes no
stored data, and it reports only persisted facts — it performs no
advancement, approval, recovery, or replay of its own, so those operations
never change what a metrics query observes beyond the facts they themselves
recorded. A missing or empty `X-Tenant-Id` is a `400 validation_error`, and
the metrics follow the usual tenant isolation: another tenant's facts are
never visible in any dimension and never affect this tenant's counts. A
tenant with no recorded facts at all gets the definite empty result with
every dimension at zero — not an error. With no query parameters the answer
is every fact recorded for the tenant, byte for byte.

The query accepts two optional query parameters, each an ISO-8601 UTC
timestamp ending in `Z`:

```http
GET /metrics?since=2026-09-26T08:00:00.000Z&until=2026-09-26T09:00:00.000Z
X-Tenant-Id: acme
```

- `since` and `until` bound a **closed** interval on the facts' occurrence
  times: a fact whose time equals `since` or `until` is counted. When either
  parameter is absent the corresponding bound is open, so omitting both
  counts every recorded fact;
- a malformed `since` or `until`, a repeated parameter, or any unknown query
  parameter is a `400 validation_error`;
- when `since` is later than `until` the request is not an error: the window
  simply contains no facts, so every dimension reports its definite zero
  result.

Each dimension is windowed by the occurrence time of its own fact: node
completions, failures, and retry consumption use the time of their recorded
node events; delivery outcomes use the delivery record's `occurred_at`
(shared by every attempt recorded in it); schedule triggers use the settled
period's trigger time; and the status distribution counts an execution at
the time it entered its current status — a still-running execution at its
start time, and a completed or terminated execution at the time of its
completion or termination event.

### Export metrics in Prometheus text format

```http
GET /metrics/export
X-Tenant-Id: acme
```

A read-only tenant-scoped entry point that returns the same counts as the
metrics query — including the same optional `since` and `until` filters, the
same closed-interval semantics, the same definite zero result for a window
with `since` later than `until`, and the same `400 validation_error` for a
missing or empty tenant, a malformed timestamp, or an unknown query
parameter — rendered in the Prometheus text exposition format
(`text/plain; version=0.0.4`). It writes no events and no usage records.

Each top-level metrics dimension is one metric family named with the
dimension's public key prefixed by `chronicleflow_`. The families appear in
the documented top-level key order, and the samples within a family are
ordered by ascending label value:

- `chronicleflow_status_distribution` — samples carry a `status` label
  (`running`, `completed`, `terminated`); only the terminated samples also
  carry a `reason` label naming one of the four termination reasons;
- `chronicleflow_node_completions`, `chronicleflow_node_failures`, and
  `chronicleflow_retry_consumption` — samples carry a `node` label;
- `chronicleflow_schedule_triggers` — samples carry a `workflow` label;
- `chronicleflow_delivery_succeeded` and `chronicleflow_delivery_failed` —
  unlabeled scalar samples.

Every value is a decimal integer. A dimension with no facts in the window
still exposes a zero-valued sample line (the labeled families expose one
unlabeled `0` sample). Every line ends with a newline and the whole document
ends with a single newline.

The top-level keys appear in this order — status distribution, node
completions, node failures, retry consumption, delivery successes, delivery
failures, and schedule triggers:

```json
{"status_distribution":{"running":1,"completed":2,"terminated":{"cancelled":0,"rejected":1,"retries_exhausted":0,"timeout":0}},"node_completions":{"charge":3},"node_failures":{"charge":2},"retry_consumption":{"charge":1},"delivery_succeeded":4,"delivery_failed":1,"schedule_triggers":{"nightly-orders":2}}
```

- `status_distribution` — the tenant's executions divided into `running`,
  `completed`, and `terminated`; `terminated` is further broken down by
  termination reason, with every reason (`cancelled`, `rejected`,
  `retries_exhausted`, `timeout`) always present, zero when no execution
  ended for that reason.
- `node_completions` — persisted node completion facts per node identifier.
  Condition evaluations and skips are not completions and are never counted;
  a loop body node counts once per iteration it completes.
- `node_failures` — submitted task failures per node identifier; a loop body
  node accumulates each iteration's failures.
- `retry_consumption` — the number of times a failed task was re-queued for
  another attempt, per node identifier.
- `delivery_succeeded` and `delivery_failed` — outbound delivery attempts by
  outcome. Every attempt counts exactly once, so a delivery that fails and
  then succeeds on a later attempt records one of each.
- `schedule_triggers` — schedule periods that created an execution, per
  workflow identifier; settling the same period again counts nothing more.

All counts are non-negative integers. A dimension with no facts reports zero
(or an empty group) rather than being omitted, and the groups within a
dimension are ordered by ascending identifier. Migrating an execution to
another version does not change attribution: facts recorded before and after
the migration accumulate under the same names. The optional `since` and
`until` filters and the Prometheus text export are described above.

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
integer between 1 and 100. The loop body is the repeated segment anchored by
the entry task and the condition: it is the entry task, the condition, and
every node either anchor reaches through `depends_on`. The two anchors must
be connected — the condition gates the entry task (the entry depends on it)
or the condition is evaluated after the entry task (it depends on the entry),
as in the example above. The body keeps the usual DAG rules and always
contains the referenced condition. The loop's own dependencies must be
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
the loop node and releases the dependencies of its successors. Once the loop
has finished, the execution-level `completed_nodes` list also includes every
body node that completed in any iteration, each listed only once and in the
order of its first completion, followed by the loop node itself; body nodes
skipped by a `run_if` guard remain out of the execution-level completed list
and are visible only in their iteration record. Body task outputs still
belong only to their iterations.

Execution state exposes each loop under `loops`: `status`, the
`current_iteration`, one entry per iteration with its own `completed_nodes`,
`skipped_nodes`, `condition_results`, and `outputs`, and the single
`end_reason` (`condition_false` or `iteration_limit`). The event stream
records `iteration_started`, `loop_condition_evaluated`, and `loop_completed`
events alongside the usual per-node ones, so replay rebuilds loop state
exactly.

### Dynamic map nodes

A `map` node expands at runtime into one task instance per element of a
recorded array:

```json
{"id": "ship", "kind": "map", "depends_on": ["collect"],
 "source": "collect", "path": "items", "max_instances": 100,
 "template": {"id": "ship-item", "retries": 2}}
```

A map node contains exactly `id`, `kind`, `depends_on`, `source`, `path`,
`max_instances`, and `template`; any other field is rejected as unknown.

- `source` names a `task` node that is also listed in `depends_on`. Expansion
  begins only after every dependency has completed or been skipped; the
  element list comes solely from the source task's recorded output.
- `path` is a non-empty dot-separated path into that recorded output, using
  the same path syntax as a condition.
- `max_instances` is a positive integer bounding how many elements may
  expand.
- `template` describes the single task each instance runs and contains
  exactly an `id`, and optionally `retries` (an integer between 0 and 10,
  defaulting to 0) and an `approval` point with the same shape a task node
  uses. The template id only names the dynamically expanded instances; it
  may coincide with a declared node id or with another map's template id.
  A map node may also sit inside a loop body, expanding independently in
  every iteration (see "Maps inside loop bodies" below).

When the dependencies settle, the value at `path` is read once. When it is an
array, one instance is created per element; the instances queue in ascending
element index order. Each `advance` still settles exactly one ready item, so
instances are worked in index order, one per call. An instance is an ordinary
task in every other respect: a submitted failure consumes the template's
retry bound and re-queues that instance, and a template approval point parks
the execution with the instance's `map_id` and `index` recorded, until an
approver decides it. Retries and approvals are per instance: results never
overwrite another instance.

When every instance is completed, the map node completes and its output,
recorded both in the node's `outputs` entry and under its `maps` record, is
the list of instance outputs in ascending element order; its successors then
become available in the usual way.

A value that is missing or is not an array expands to zero instances: the map
completes with an empty output list and releases its successors immediately.
When the array holds more elements than `max_instances`, no instance is
created at all: the map node fails permanently, is listed under
`failed_nodes`, and the execution terminates with reason `retries_exhausted`
under the usual exhaustion semantics.

Execution state exposes each map under `maps`: the node's `status`
(`pending`, `running`, `completed`, or `failed`), one record per retained
instance giving its `index`, `status` (`ready`, `waiting`, `completed`, or
`failed`), its `output`, and its `failure_reason` (null except for a
permanently failed instance), plus the ordered completed `outputs` list and
the node's single `failure_reason`. A `ready` instance that has been
rewritten by a successful modify additionally carries that rewritten
`input`; every other instance record keeps exactly these fields. The `maps`
field is present only when the bound workflow declares a map node outside
any loop body; every other execution keeps its previous state shape.

The event stream records a `map_expanded` event when expansion happens,
carrying the map id and the number of created instances (zero for a missing
or non-array value, and zero together with `exceeded` and the observed
element count when the bound is exceeded), and a `map_completed` event with
the map id and final instance count. Each instance's per-node events —
`node_completed`, `node_failed`, and `node_retried`, plus
`approval_requested` and `approval_decided` for a template approval point —
carry their `map_id` and element `index` alongside the template node id, so
replay rebuilds every instance solely from the event stream. Checkpoints are
written at instance boundaries, so recovery resumes with the unfinished
instances and never repeats an instance output. Per-instance completions and
failures count toward the existing node metrics under the template node id,
and webhook and queue subscriptions receive instance events exactly like any
other node event. Instance deletion and modification add their own
`instance_deleted` and `instance_modified` events, and re-expansion adds a
`map_reexpanded` event, described under
"Delete and modify expanded instances" below.

### Maps inside loop bodies

A map node may belong to a loop body — it is part of the body exactly when
the loop's entry or condition reaches it through `depends_on`, like any
other body member:

```json
{"id": "fanout", "kind": "map", "depends_on": ["attempt"],
 "source": "attempt", "path": "items", "max_instances": 10,
 "template": {"id": "work", "retries": 1}}
```

A nested map expands once per iteration, when every dependency of its node
is completed or skipped in that iteration, and the element list is read from
the source task's output of that same iteration. The expansion rules are the
ones of an execution-level map: a missing path or a non-array value expands
to zero instances and the map completes for that iteration with an empty
output list, so the loop boundary is evaluated as usual; an element count
beyond `max_instances` creates no instance at all — the map fails
permanently, is listed under `failed_nodes`, and the execution terminates
with reason `retries_exhausted` under the usual exhaustion semantics.

The expanded instances belong to their iteration only: they queue in
ascending element index order and are settled one per `advance`, exactly
like execution-level instances, and one iteration's instances never
overwrite another iteration's results. An instance is an ordinary task in
every other respect — a submitted failure consumes the template's retry
bound and re-queues that instance within its iteration, and a template
approval point parks the execution until an approver decides it — with no
influence from the iteration number.

Iteration state exposes each nested map under the iteration record's `maps`
field, with the same per-map shape as an execution-level map (`status`, one
record per instance, the ordered completed `outputs`, and `failure_reason`);
the field is present on every iteration of a loop whose body declares a map
node. When a nested map completes, its node enters the iteration's
`completed_nodes` and its output list is recorded in the iteration's
`outputs`; when the loop finishes, the usual roll-up lists the map node once
in the execution-level `completed_nodes`, while its outputs stay with their
iterations. An execution-level `maps` field is not created for nested maps.

Every event a nested expansion records carries the owning `loop_id` and
`iteration` alongside the map id and, for per-instance events, the element
`index`: `map_expanded`, `map_completed`, the per-instance `node_completed`,
`node_failed`, and `node_retried`, and `approval_requested` and
`approval_decided` for a template approval point. The `iteration_started`
event of a loop whose body declares map nodes carries the iteration's
initial `maps` skeleton, so replay rebuilds every iteration's instance list,
status, and ownership solely from the event stream, consistent with the
materialized state. Checkpoints are written at each iteration's instance
boundaries, so recovery continues the current iteration's unfinished
instances and never repeats an instance output. Cancellation, timeouts,
leases, retries, and approvals keep their existing semantics, and a nested
map never changes an execution's termination reason.

Executions whose workflows declare no nested map behave exactly as before:
state fields, event contents, and advancement results are unchanged.

### Delete and modify expanded instances

An expanded dynamic node offers, alongside expansion and per-instance
advancement, two instance-level operations and a node-level re-expansion.
All three are ordinary idempotent HTTP commands over the same entry points
the rest of the service uses; they introduce no second protocol.

An instance that has never advanced can be removed, and so can an instance
that has permanently failed:

```http
POST /executions/run-1/maps/ship/instances/2/delete
Idempotency-Key: delete-instance-2

{}
```

The body must be an empty object. Removing an instance drops only that
instance's record: every retained instance keeps its element index, its
place in the queue, and its advancement order. When the removed instance was
the map's last unfinished instance, the node completes under the existing
rules with an output list containing only the retained instances' outputs,
in ascending element-index order, and its successors unlock as usual. An
instance that is waiting on an approval, or has already completed, can never
be deleted; attempting it returns `409 conflict` and changes no state. A
permanently failed instance is removable after the execution has terminated,
but removing it never revives the execution, alters its termination reason,
or changes the node's failed conclusion. When every retained instance of a
map inside a loop body has ended, the iteration's other nodes advance as
usual and the node completes or ends under the existing rules; deleting an
instance in one iteration never touches another iteration.

An instance that has never advanced can also have its input rewritten
before it is worked:

```http
POST /executions/run-1/maps/ship/instances/1/modify
Idempotency-Key: modify-instance-1

{"input":{"address":"742 Evergreen Terrace"}}
```

The body must contain exactly `input`, an object with the new content; it is
validated like every other request body (finite numbers only). The rewrite
changes only the parameters used when that one instance is later advanced:
it affects no other instance, and the output and history of an instance that
has already advanced (waiting, completed, or permanently failed) are
immutable — modifying such an instance returns `409 conflict` and changes no
state. The rewritten input, including its full float precision and `-0.0`,
is stored verbatim.

Delete and modify calls on the same instance settle in call order: every
call applies to the instance list exactly as the earlier calls left it, and
several calls on one instance each take effect independently, one after
another. A rewritten instance has still never advanced, so it remains
deletable; deleting or modifying an instance an earlier call already
removed is a `404 not_found`; and a second rewrite of the same instance
records the first rewrite's content as its `before`.

Both commands apply to a map nested in a loop body exactly as to an
execution-level map: the current running iteration's instances are
addressed by the same path, and every event they append carries the owning
`loop_id` and `iteration` alongside the map id; an execution-level instance
carries no loop context.

An out-of-bounds element index, a reference to a node that is not a dynamic
`map`, a missing execution, or a cross-tenant reference is a `404 not_found`
and writes nothing. A missing or mistyped field, a non-object modify
`input`, an unknown field, or a non-finite number is a `400 validation_error`
with no partial write. Reusing an idempotency key for another operation is a
`409 conflict`; repeating the same command with the same key returns the
first result and appends no second event.

Every successful call appends exactly one event to the execution stream:

- an `instance_deleted` event records the action type, the owning dynamic
  node under `map_id`, the removed instance's element `index` and `status`
  (`ready` or `failed`), and, for a map inside a loop body, the `loop_id`
  and `iteration`;
- an `instance_modified` event records the action type, the owning
  `map_id`, the instance `index`, the content `before` the rewrite (`null`
  for an instance that had never been rewritten) and `after` it, and the
  same loop context when the instance belongs to an iteration.

Replay rebuilds the instance list, every instance's status and ownership,
and each rewrite solely from the event stream: a delete drops the recorded
instance (and the subsequent `map_completed`, when the deletion finished the
node, completes it with the retained outputs), and a modify applies the
rewritten input to that one instance, so the rebuilt state is exactly the
materialized state. A checkpoint is written at the instance boundary of
every successful delete or modify; after a restart recovery continues the
unfinished instances, repeats no instance output, and never resurrects a
deleted instance. The operations change no advancement conclusion or
termination semantics: cancellation, timeouts, approvals, leases, retries,
and retry exhaustion keep their existing behavior. Executions that never
use deletion, modification, or re-expansion keep exactly their current
state fields, event contents, and advancement results.

### Re-expand a dynamic node

A dynamic node that has already expanded can be expanded again on demand —
above all to regenerate its instance list after deletions:

```http
POST /executions/run-1/maps/ship/reexpand
Idempotency-Key: reexpand-1

{}
```

(The same operation is also accepted at `.../maps/ship/expand`.) The body
must be an empty object. Re-expansion reads the element list again from the
source task's recorded output — the same recorded output the first
expansion read, which never changes once recorded — and regenerates the
instance list from it: every previous instance record is replaced, and the
fresh instances queue from element index zero in ascending order. The
regenerated instances are ordinary instances in every respect: they are
deleted, modified, advanced, retried, and approved through the same entries
and with exactly the same behavior as a first expansion, and their retry
bounds start from zero.

Every successful call appends exactly one `map_reexpanded` event, recording
the action type, the owning dynamic node under `map_id`, and the number of
generated instances under `instance_count`; for a map inside a loop body
the event also carries the owning `loop_id` and `iteration`, and only the
current running iteration's expansion is replaced. A node that had
completed returns to `running`: it leaves `completed_nodes`, its recorded
output entry is discarded, and when the regenerated instances have all
completed the node completes again under the existing rules, its result
list holding only the finally retained instances' outputs in ascending
element-index order. An empty element list completes the node again at
once with an empty output list, exactly like a zero-element first
expansion.

Re-expansion is rejected with `409 conflict` and writes nothing when the
node has not expanded yet, when it has permanently failed, when one of its
instances is waiting on an approval decision, when the execution has
already completed, or when a nested map's loop is not currently running.
A terminated execution's unfinished node may still be re-expanded; doing so
never revives the execution or alters its termination reason. A missing
execution, a reference to a node that is not a dynamic `map`, or a
cross-tenant reference is a `404 not_found` and writes nothing. A
non-empty or non-object body, an unknown field, or a non-finite number is a
`400 validation_error` with no partial write. Reusing an idempotency key
from another operation is a `409 conflict`; repeating the same call with
the same key returns the first result and appends no second event. Replay
rebuilds the regenerated instance list, statuses, and ownership solely
from the event stream, a checkpoint is written at the re-expansion
boundary, and recovery after a restart continues the unfinished
regenerated instances — repeating no instance output and never resurrecting
an instance that stayed deleted.

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

### Migrate a running execution to another version

```http
POST /executions/run-v1/migrate
Idempotency-Key: migrate-request-1

{"version":"2026-09-25"}
```

A running execution can be explicitly moved to another declared version of
the same workflow; after the call it keeps advancing under the new version.
The body must contain exactly `version`, a non-empty version identifier that
names a stored version of the execution's workflow. On success a
`version_migrated` event is appended to the event stream — it records the
`from_version`, the `to_version`, and the structural skeleton of the target
revision (its `loops`, and, when it declares approval points, its
`waiting_approval` and `approvals` fields) — and a checkpoint of the rebound
state is written at that same node boundary. The execution's state then
carries the target version.

Everything that follows uses the target version's definition: ready-node
selection, condition evaluation, loop rounds, approval points, retries, and
the workflow subscriptions that fire for later events. Everything recorded
before the migration is kept exactly as-is: prior events, node outputs,
condition conclusions, and per-iteration history are never rewritten. Loops
introduced by the target definition begin in their initial pending state; a
loop the old definition knew keeps its recorded iterations and conclusion.
Replay rebuilds the migration point and version binding solely from the
event stream, and after a restart recovery continues on the migrated version
from the migration checkpoint, so checkpoints and materialized state agree.

An execution parked at an approval point may also be migrated: the waiting
point keeps its recorded node and approver list unchanged (only a recorded
approver may still decide it), and once the decision lands the subsequent
advancement follows the target version. Executions that are never migrated
keep advancing on the version they started with to completion or
termination; adding versions never moves them. An execution of a workflow
that never declared a version cannot name a target, and such workflows and
their executions keep exactly the previous behavior, state shape, and event
content.

The response is the updated execution state. Migrating a `completed` or
`terminated` execution returns `409 conflict` and changes nothing. A missing
execution or a target version that does not exist for the workflow returns
`404 not_found`; a cross-tenant reference is the same missing resource. A
missing or mistyped `version`, an unknown field, or a non-finite number is a
`400 validation_error`. Migrating a running execution to the version it is
already bound to returns the current state as-is, appending no event and
writing no checkpoint; a `completed` or `terminated` execution instead
returns `409 conflict` even when the target version is exactly the one it is
bound to. Reusing an idempotency key for another operation is a `409
conflict`. The migration event is listed directly by the per-execution
events query, and node outputs produced after the migration belong to the
target version's definition. Approver-list validation, lease renewal, and
delivery history keep their existing semantics before and after a migration.

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

### List workflows

```http
GET /workflows?limit=50
X-Tenant-Id: acme
```

Enumerates the workflows declared in the request's namespace. The response
is `{"workflows":[{"id":"order-flow"}, ...]}`: one entry per workflow, each
carrying its identifier, in ascending identifier order. A namespace with no
workflows gets the definite empty result `{"workflows":[]}`, not an error.
The query is read-only: it creates nothing, appends no events, and records
no usage, so repeating it returns the same answer and changes no stored
state.

The list is paged with a keyset cursor:

- `limit` — required; the maximum number of entries in the page, a positive
  integer. The page never holds more entries than the limit;
- `cursor` — optional; the identifier of the previous page's last entry.
  Only entries whose identifier sorts strictly after the cursor are
  returned, so paging with the previous page's last identifier yields
  consecutive pages that neither overlap nor skip. A cursor past the end
  returns the definite empty list.

### List executions

```http
GET /executions?workflow_id=order-flow&status=terminated&termination_reason=timeout&since=2026-09-26T08:00:00.000Z&until=2026-09-26T09:00:00.000Z&cursor=run-41&limit=50
X-Tenant-Id: acme
```

Enumerates the executions in the request's namespace in ascending execution
identifier order. The response is `{"executions":[...]}`; each entry carries
the execution `id`, its `workflow_id`, its `status` (`running`, `completed`,
or `terminated`, always a string), its `termination_reason` (one of the
four termination reasons for a terminated execution, otherwise `null`), and
its `created_at` (the ISO-8601 UTC creation timestamp ending in `Z`), in
that stable key order. A namespace with no executions gets the definite
empty result `{"executions":[]}`. Like the workflows list this query is
read-only and never settles a due timeout, appends an event, writes a
checkpoint, or records usage, so repeated calls return the same answer and
change no state or metering conclusion.

The same `limit` and `cursor` pagination applies, with the cursor taken
from the previous page's last execution identifier. In addition, five
filters may be combined, and every given filter must match:

- `workflow_id` — only executions of that workflow. Referencing a workflow
  the namespace does not contain, including one owned by another tenant, is
  the usual `404 not_found` and reveals nothing about its existence;
- `status` — only executions in that lifecycle status. A value other than
  `running`, `completed`, or `terminated` is a `400 validation_error`;
- `termination_reason` — only executions that terminated for exactly that
  reason. The filter is meaningful solely for terminated executions:
  running and completed executions never appear under it, even when other
  filters are given alongside it. An unknown reason is a
  `400 validation_error`;
- `since` and `until` — the same closed creation-time bounds the events and
  metrics queries accept: each is an ISO-8601 UTC timestamp ending in `Z`,
  and an execution whose `created_at` equals `since` or `until` is
  returned. When either parameter is absent the corresponding bound is
  open, so omitting both returns exactly the unfiltered list; a malformed
  timestamp is a `400 validation_error`. When `since` is later than
  `until` the window matches nothing and the result is the definite empty
  list, not an error.

Both list endpoints follow the usual tenant scope: omitting
`X-Tenant-Id` keeps the single legacy namespace, and a tenant sees only its
own workflows and executions under every combination of filters. Every
parameter may appear at most once; a repeated parameter, an unknown
parameter, a missing `limit`, or a `limit` that is not a positive integer is
a `400 validation_error` that writes nothing.

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

The events query is read-only: it appends no events, records no usage, and
changes no state, so advancement, approvals, recovery, and replay never alter
what it observes. With no query parameters it returns the complete ordered
event list exactly as before. It also accepts optional filter and pagination
parameters, which combine as an intersection:

```http
GET /executions/run-1/events?types=node_completed,node_failed&since=2026-09-26T08:00:00.000Z&until=2026-09-26T09:00:00.000Z&cursor=12&limit=50
```

- `types` — a comma-separated set of event types; only events whose type is
  in the set are returned. An empty entry, a duplicate entry, or an unknown
  event type is a `400 validation_error`;
- `since` and `until` — the same closed-interval occurrence-time bounds the
  metrics query accepts: an event whose time equals either boundary is
  returned. When `since` is later than `until` the window matches nothing and
  the result is an empty list, not an error;
- `cursor` — the sequence of the last event of the previous page; only events
  with a strictly greater sequence are returned;
- `limit` — the maximum number of events in the page, in ascending sequence
  order.

Every parameter may appear at most once; a repeated parameter, an unknown
parameter, a malformed timestamp, or a cursor or limit that is not a positive
integer is a `400 validation_error` that writes nothing. Filtering never
renumbers events, so paging with `cursor` set to the previous page's last
sequence yields consecutive pages that neither overlap nor skip. A filter
that matches nothing returns the definite empty result `{"events":[]}`, and
querying a missing or another tenant's execution is the usual
`404 not_found`.

### Webhook notifications

Subscriptions declared on a workflow or an execution deliver outbound webhook
messages when business events occur. Each webhook subscription contains exactly:

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
workflow, execution, or subscription is partially written. A subscription
may instead declare a queue target (`queue` instead of `url`); see "Queue
subscriptions" below.

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

### Queue subscriptions

A subscription may declare a queue target instead of a webhook `url`:

```json
{
  "id": "run-4",
  "workflow_id": "order-flow",
  "input": {},
  "subscriptions": [{"queue": "order-events", "events": ["node_completed"], "visibility_seconds": 60}]
}
```

A queue subscription contains exactly:

- `queue`: a non-empty queue name of any length;
- `events`: the same non-empty event-type list a webhook subscription takes;
- `visibility_seconds` (optional): a positive number of seconds a pulled
  message stays invisible while awaiting acknowledgement, defaulting to 30.

Exactly one of `url` and `queue` must be present in a subscription. Any
other field, a missing or invalid `events` list, an empty queue name, or a
non-positive or non-finite visibility timeout is a 400 `validation_error`
that rejects the whole request without partial writes. Queue names are
unique per tenant across declarations: declaring a name that another
workflow or execution of the same tenant already declared is a 409
`conflict`, while different tenants may reuse the same names freely.

A workflow or an execution may declare several queue targets. Every
execution gets its own queues — those declared on the workflow revision it
is bound to, plus those it declares itself — and when a subscribed event
occurs, a message carrying the event type and its details is appended to
each matching queue of that execution, in occurrence order. The append is
atomic with the event itself: a rejected operation leaves no queued message
behind. Each message carries an idempotency key that is deterministic per
event and queue: every delivery of the same message reuses the same key,
and different events never share a key. Queues add no fields to the
execution state and append no events, and replay, recovery, and queries
never enqueue. Executions that declare no queue targets behave exactly as
before.

### Inspect the queue state

```http
GET /executions/run-4/queues
X-Tenant-Id: acme
```

Returns `{"queues": [...]}` with one entry per queue of the execution, in
declaration order; an execution with no queue targets gets the definite
empty result `{"queues": []}`. Each entry gives its `queue` name, the
`visibility_seconds` in effect, and the queue's `messages` in entry order —
an empty list for a queue that never received a message. Each message record
gives its `sequence` (the entry order), the `event_type` and
`event_sequence` that produced it, the event `payload`, the message
`idempotency_key`, the `delivery_count`, a `deliveries` list recording every
delivery with its `delivery` number and `delivered_at` time, the
`enqueued_at` time, and the current `status` (`pending`, `delivered`, or
`acknowledged`). Querying the queues of a missing execution — including one
owned by another tenant — is the usual 404 `not_found`.

### Pull messages from a queue

```http
POST /executions/run-4/queues/order-events/pull
Idempotency-Key: pull-request-1
X-Tenant-Id: acme

{}
```

The body must be an empty object. The response is `{"messages": [...]}`: the
currently pending messages in entry order, each rendered like the message
records of the queue state query. A pulled message becomes invisible for the
queue's visibility timeout; if it is not acknowledged by then, it returns to
the pending set and is delivered again — the queue semantics are at least
once, and every delivery is recorded in the message's delivery history.
Pulling an empty queue, or one whose messages are all invisible, is a
definite empty result `{"messages": []}`, not an error. Every delivery to
the caller is metered as one `delivery_attempted` usage record, exactly like
an outbound webhook attempt, and counts toward the tenant's bill.

### Acknowledge a message

```http
POST /executions/run-4/queues/order-events/ack
Idempotency-Key: ack-request-1
X-Tenant-Id: acme

{"idempotency_key": "run-4:2:queue:order-events"}
```

The body must contain exactly `idempotency_key`, naming a message of the
queue. Acknowledgement is explicit and final: the message permanently leaves
the queue and is never delivered again, and the response is
`{"acknowledged": true}`. Acknowledging a message that was already
acknowledged, or a key the queue does not know, is a 404 `not_found`.
Pulling from or acknowledging a queue the execution does not have, or one
belonging to a missing or cross-tenant execution, is likewise a 404
`not_found`. Pull and acknowledge are idempotent commands like every other
operation: repeating one with the same `Idempotency-Key` returns the
original result, and reusing a key across different operations is a 409
`conflict`.

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

Returns a single `schedule` object with the status fields, or the definite
empty result `{"schedule":null}` for a workflow that never declared a
schedule. When a schedule exists the object is:

```json
{
  "schedule": {"interval_seconds": 3600, "input": {"mode": "nightly"}, "missed_policy": "catch_up"},
  "paused": false,
  "last_triggered_at": "2026-09-26T08:00:00.000Z",
  "last_execution_id": "nightly-orders-scheduled-i:42"
}
```

The fields are:

- `schedule`: the declared plan exactly as stored — either
  `{"interval_seconds": <positive integer>, "input": <object>, "missed_policy": "catch_up"|"skip"}`
  or `{"cron": "<five-field expression>", "input": <object>, "missed_policy": ...}`;
  the input is the declared object verbatim;
- `paused`: a JSON boolean;
- `last_triggered_at`: an ISO-8601 UTC timestamp string ending in `Z`, or
  `null` until the first period fires;
- `last_execution_id`: the execution id string of the most recently created
  run, or `null` until the first trigger.

`last_triggered_at` and `last_execution_id` are `null` (not omitted) until
the first trigger. A missing workflow returns 404 `not_found`.

### Preview upcoming triggers

```http
GET /workflows/nightly-orders/schedule/preview?limit=3
```

Returns the trigger times the declared plan would settle next, starting
from the first period that has not been settled yet, together with the
input each created execution would carry:

```json
{
  "schedule": {"interval_seconds": 3600, "input": {"mode": "nightly"}, "missed_policy": "catch_up"},
  "previews": [
    {"trigger_at": "2026-09-27T09:00:00.000000Z", "input": {"mode": "nightly"}},
    {"trigger_at": "2026-09-27T10:00:00.000000Z", "input": {"mode": "nightly"}},
    {"trigger_at": "2026-09-27T11:00:00.000000Z", "input": {"mode": "nightly"}}
  ]
}
```

- `schedule`: the declared plan exactly as stored, like the status query;
- `previews`: up to `limit` entries in ascending time order, each carrying
  `trigger_at` (an ISO-8601 UTC timestamp string ending in `Z`) and `input`
  (the declared input object verbatim).

A fixed interval projects continuously from the next unsettled period; a
cron plan takes the next minute matching its field rules, then the one
after that, and so on. The `limit` query parameter is required, must be a
positive integer, and may appear at most once; a missing, repeated, or
malformed value — or any other query parameter — is a 400
`validation_error`. The preview is read-only: it creates no execution,
appends no event, and never moves the schedule's cursor, so repeating the
query returns the same projection. A paused schedule projects exactly like
a running one — the missed policy only governs whether a due period
creates an execution, never the projection. A workflow that never declared
a schedule returns the definite empty result `{"schedule":null}`, exactly
like the status query; a missing or cross-tenant workflow returns 404
`not_found`.

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

### Schedule change history

Every successful schedule declaration, replacement, pause, and resume leaves
one change record, so a caller can see exactly when each scheduling change
took effect; every other operation — reads, previews, and the automatic
triggering of due periods included — leaves no record. The history hangs off
the same schedule entry point the status and preview use, with the same
calling shape as the price-table change history:

```http
GET /workflows/nightly-orders/schedule/history?limit=50
X-Tenant-Id: acme
```

Returns `{"history":[...]}` with the records in ascending order of occurrence
time; records sharing one instant are ordered by a stable ascending sequence.
Each record is:

```json
{"sequence":7,"action":"declare","occurred_at":"2026-09-26T08:30:00.000Z","snapshot":{"schedule":{"interval_seconds":3600,"input":{"mode":"nightly"},"missed_policy":"catch_up"},"paused":false}}
```

- `sequence` is a stable, strictly increasing positive integer per tenant and
  workflow, shared across every kind of schedule change. It identifies the
  record across every query and is never renumbered by a filter or a page;
- `action` is `declare` for a schedule declaration or a replacement (including
  one carried by workflow creation or an added version), `pause` for a pause,
  or `resume` for a resume;
- `occurred_at` is the moment the change took effect, an ISO-8601 UTC string
  ending in `Z`;
- `snapshot` is the complete schedule in effect after the change, listing
  `schedule` — the declared plan exactly as stored, the plan text preserved
  verbatim and never rewritten (a cron expression stays its original
  five-field text, and the input keeps its original content) — followed by
  `paused`, the pause flag then in effect. A declaration or replacement keeps
  the previous pause flag, so a replacement made while paused snapshots
  `"paused":true`; a pause snapshots `true` and a resume `false`.

The history record is written atomically with the change it describes, in the
same transaction as the schedule write, so a reader always sees the schedule
and its history agree. A declaration rejected by validation, a pause or resume
of a missing workflow or one without a schedule, an idempotency-key conflict,
or a key reused for another operation leaves no record. Repeating the same
declaration, replacement, pause, or resume with the same key returns the first
result, byte for byte, and appends no second record; reusing a key for another
operation is the usual `409 conflict` and, again, writes no history.

A tenant with no matching records gets the definite empty result
`{"history":[]}`, never an error. The response is one line of compact JSON
with the record keys in the order above, full number precision preserved
(including `-0.0`), and a single trailing newline, like every other JSON
endpoint.

The endpoint accepts an optional action filter, the same optional closed time
window the per-record usage query accepts, plus cursor pagination, in exactly
the same shape as the price-table change history:

```http
GET /workflows/nightly-orders/schedule/history?action=declare,pause,resume&since=2026-09-26T08:00:00.000Z&until=2026-09-26T09:00:00.000Z&cursor=12&limit=50
```

- `action` names one action or a comma-separated set of them (`declare`,
  `pause`, and `resume`); only records whose action is in the set are
  returned. A single value needs no comma. An empty entry, a duplicate entry,
  or an unknown action is a `400 validation_error`; when `action` is absent
  every change is returned;
- `since` and `until` are ISO-8601 UTC timestamps ending in `Z` and bind a
  **closed** interval on each record's occurrence time: a record whose time
  equals either boundary is included. When either is absent the corresponding
  bound is open; when `since` is later than `until` the window matches nothing
  and the definite empty list is returned;
- `limit` is **required** and must be a positive integer; a page never
  contains more than that many records;
- `cursor` is the `sequence` of the previous page's last record; only records
  whose `sequence` is strictly greater are returned, so pages neither overlap
  nor skip. It is omitted on the first page and must be a positive integer;
- the action filter, time window, and pagination apply together as an
  intersection, and filtering never changes a record's `sequence`.

A malformed timestamp, an unknown, empty, or duplicated `action` entry, a
missing or non-positive-integer `limit`, a non-positive-integer `cursor`, a
repeated parameter, or any unknown query parameter is a `400
validation_error` that writes nothing. A missing or empty `X-Tenant-Id` is a
`400 validation_error` that reveals no records. Querying the history of a
missing workflow, or of a workflow owned by another tenant, is answered
exactly like a missing workflow: `404 not_found`, with no indication that the
workflow exists elsewhere; another tenant's records are never visible under
any action filter, window, or page. The query is read-only: it appends no
history, writes no metering record, settles no due period, and changes no
schedule declaration, trigger cursor, usage, bill, quota, approval, or replay
conclusion. The history entry point changes no existing behavior: declaring or
replacing a schedule, pausing and resuming it, and the automatic triggering of
due periods are byte-for-byte unchanged apart from the additional history row
the successful write now commits.

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
body may additionally carry `scope`, either `execution` (the default when
omitted) or `target`. With `scope` omitted or `execution`, the claim takes the
single execution-wide lease described here: the response contains the claimed
`work_item` (its `execution_id` and `workflow_id`) and a `lease` recording the
`worker_id`, the `lease_seconds` duration, the `expires_at` deadline, and the
`heartbeat_at` active time. Each execution work item is held by at most one
worker at a time: claiming a work item whose lease is still active — even by
the same worker — is a 409 `conflict`. Claiming a completed or terminated
execution returns the definite empty result
`{"work_item":null,"lease":null}` and absorbs no input, and claiming a missing
or another tenant's execution returns 404.

#### Parallel target leases

With `"scope":"target"` a claim does not take the execution-wide lease.
Instead it leases, in ready order, one independent ready target that no other
valid target lease currently holds, so several workers can settle different
ready targets of the same execution in parallel. The returned `work_item`
always contains `execution_id`, `workflow_id`, and the target's `node_id`; a
map instance additionally carries `map_id` and the element `index`, and an
instance of a map inside a loop body also carries the owning `loop_id` and
`iteration`. The `lease` contains `work_item_id`, `worker_id`,
`lease_seconds`, `expires_at`, and `heartbeat_at`; the two times are ISO-8601
UTC strings ending in `Z`.

Re-claiming while a valid target lease still exists for every ready target is
a 409 `conflict` (re-claiming a validly held target, even by the same worker).
When no target can be leased — the execution has finished, is parked at an
approval point with no other ready target, or simply has no ready target — the
claim returns the same definite empty result
`{"work_item":null,"lease":null}`. A missing or cross-tenant execution is a 404
`not_found`, as is a finished execution's claim. A missing or empty
`worker_id`, a `scope` other than `execution` or `target`, an unknown field, or
a non-positive, non-numeric, or non-finite `lease_seconds` is a 400
`validation_error` that writes nothing.

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

##### Settling a target lease

A target lease is settled through the same `advance` entry, but the body must
contain exactly one of `output` or `failure`, plus `worker_id` and
`work_item_id`; only that one target is settled. Completing its output marks
the target's successors ready under the usual ordering, and a submitted
failure keeps the target's retry semantics (re-queue with `node_failed` and
`node_retried` while retries remain, otherwise terminating the execution) and
the existing map and loop event semantics. A target approval point is parked
by the advance and resolved through the usual decision entry. The target's
state, events, and checkpoint commit atomically, and the lease is released as
part of the same transaction.

`heartbeat` and `release` for a target lease take exactly `worker_id` and
`work_item_id`; a heartbeat extends only that lease and a release returns only
that target to the claimable set. Claims, heartbeats, and releases append no
events and write no checkpoint. After a lease expires or is released, the
target may be claimed again (under a new `work_item_id`); the previous holder
submitting, heartbeating, or releasing the old `work_item_id` is a 409
`conflict`, while an unknown or cross-tenant `work_item_id` is a 404
`not_found`. A missing field, an unknown field, a non-object or invalid body,
an illegal time value, or a non-finite number on these operations is a 400
`validation_error`.

While any valid target lease is active, the execution-wide mode is parked: an
old-style `advance`, `heartbeat`, or `release` that carries no
`work_item_id`, and an execution-scoped claim, all return 409 `conflict`. Once
every target lease has been released or has expired, the execution-wide mode is
available again.

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
or an execution with no claimed work item is a 404 `not_found`. A target
lease is heartbeated with a body of `worker_id` and `work_item_id` (see
"Parallel target leases"), which extends only that one lease.

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
declarations and deletes and both quota reads, price declarations and price deletes, and the usage, bill,
and metrics queries require a tenant, and quota limits and declared unit
prices are positive integers validated by the
same rules as every other body (no non-finite numbers, no unknown fields).
Reusing a workflow or execution identifier, adding a workflow version whose
tag already exists for that workflow, or reusing an idempotency key across
different operations (within the same tenant), is a conflict. Starting an
execution against a missing workflow or a missing workflow version is a
missing resource (`404 not_found`). Migrating an execution to a missing
version is likewise a missing resource, while migrating a completed or
terminated execution is a conflict; a malformed migration body is a
validation error, and a migration to the version already bound is a
state-returning no-op rather than an error.
Exceeding a tenant's declared workflow or execution quota is also a `409
conflict`, with the word "quota" in the message so it can be distinguished
from an identifier conflict; the rejected request writes nothing. Referencing
a resource owned by another tenant is a missing resource (`404
not_found`), indistinguishable from one that does not exist, so existence is
never revealed across tenants. A `retries` value that is negative, non-integer, or greater than
10, and a non-positive `timeout_seconds`, are validation errors. A malformed
subscription — a missing or mistyped field, an unknown field, an empty or
duplicated event list, an unknown event type, an empty or non-http(s) `url`,
a non-positive timeout or attempt count, more than ten attempts, a
subscription declaring both `url` and `queue` or neither, an empty queue
name, or a non-positive visibility timeout — is a
validation error that rejects the whole request without partial writes, and
querying the delivery history or the queues of a missing execution is a
missing resource. Declaring a queue name another workflow or execution of
the same tenant already declared is a conflict. Pulling from or
acknowledging a missing queue, and acknowledging an unknown or already
acknowledged message, are missing resources; a pull body that is not an
empty object, or an acknowledgement body without exactly an
`idempotency_key` string, is a validation error. An approval
point with an empty approver list, a duplicate or non-string approver, an
approval on a non-task node, and a decision body that is malformed or carries
a decision other than `approved` or `rejected` are validation errors.
A map node with an unknown or missing field, a mistyped or non-positive
`max_instances`, an invalid template, a `source` that is not a
task dependency, or an invalid `path` is likewise a validation error that
rejects the whole request, whether the map stands alone or sits inside a
loop body.
Deleting or modifying an expanded instance whose index is out of bounds,
whose map node does not exist, whose execution is missing, or that belongs
to another tenant is a missing resource; deleting an instance that is
waiting on an approval or already completed, or modifying any instance that
has already advanced, is a conflict; a delete body that is not empty, a
modify body without exactly an `input` object, and any unknown field or
non-finite number are validation errors that write nothing.
Re-expanding a node that is not a dynamic `map`, a missing execution, or a
cross-tenant reference is likewise a missing resource; re-expanding a node
that has not expanded yet, has permanently failed, has an instance waiting
on an approval, belongs to a completed execution, or sits in a loop that is
not currently running is a conflict; a re-expansion body that is not an
empty object is a validation error that writes nothing.
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
validation error. Querying the schedule change history of a missing or
cross-tenant workflow is a missing resource, and its malformed or missing
`limit`, non-positive `cursor`, unknown or duplicated `action`, malformed
timestamp, repeated or unknown query parameter, and missing or empty tenant
are validation errors, exactly like the price-table change history. Request bodies must not contain
non-finite numbers (`NaN`, `Infinity`, or overflowing values such as `1e400`);
they are rejected with 400. Finite floats keep their full precision, including negative zero
(`-0.0`), and every response body ends with a single newline.

## Tests

```bash
PYTHONPATH=src python -m unittest discover -s tests -v
```

