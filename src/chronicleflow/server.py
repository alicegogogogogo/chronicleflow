from __future__ import annotations

import argparse
import json
import math
from collections import namedtuple
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlsplit

from .errors import ChronicleFlowError, NotFoundError, ValidationError
from .service import (
    EVENT_TYPES,
    EXECUTION_STATUSES,
    TERMINATION_REASONS,
    ChronicleFlow,
    _parse_timestamp,
)

TENANT_HEADER = "X-Tenant-Id"

# The only query parameters the metrics query and the Prometheus export
# accept; anything else is a validation error exactly like an unknown field.
METRICS_QUERY_PARAMETERS = ("since", "until")

# The only query parameters the execution events query accepts: a type set, a
# closed time window, and cursor pagination. Anything else is a validation
# error exactly like an unknown field.
EVENTS_QUERY_PARAMETERS = ("types", "since", "until", "cursor", "limit")

# The only query parameter the schedule preview accepts: the number of
# projected trigger times. Anything else is a validation error exactly like
# an unknown field.
PREVIEW_QUERY_PARAMETERS = ("limit",)

# The list queries share keyset pagination (a cursor identifier and a page
# size). The workflows list accepts nothing else; the executions list also
# takes a workflow identifier, a lifecycle status, a termination reason, and a
# closed creation-time window.
LIST_QUERY_PARAMETERS = ("cursor", "limit")
EXECUTIONS_QUERY_PARAMETERS = (
    "workflow_id",
    "status",
    "termination_reason",
    "since",
    "until",
    "cursor",
    "limit",
)

# A response rendered as a non-JSON text body (the Prometheus export).
TextResponse = namedtuple("TextResponse", ("content_type", "body"))


def _reject_non_finite(constant: str) -> Any:
    raise ValidationError(f"request body must not contain {constant}")


def _positive_integer(raw: str, name: str) -> int:
    """Parse a query parameter that must be a positive integer."""
    if not raw or not all(character in "0123456789" for character in raw):
        raise ValidationError(f"{name} must be a positive integer")
    value = int(raw)
    if value < 1:
        raise ValidationError(f"{name} must be a positive integer")
    return value


def _assert_finite(value: Any) -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise ValidationError("request body must not contain non-finite numbers")
    if isinstance(value, dict):
        for item in value.values():
            _assert_finite(item)
    elif isinstance(value, list):
        for item in value:
            _assert_finite(item)


class Handler(BaseHTTPRequestHandler):
    service: ChronicleFlow

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _json(self, status: int, value: Any) -> None:
        body = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode() + b"\n"
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _write(self, status: int, response: Any) -> None:
        if isinstance(response, TextResponse):
            body = response.body.encode()
            self.send_response(status)
            self.send_header("Content-Type", response.content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self._json(status, response)

    def _metrics_window(self) -> tuple[Any, Any]:
        """Parse the optional since/until query parameters of the metrics routes.

        Both parameters are ISO-8601 UTC timestamps ending in Z, may appear at
        most once, and are the only accepted parameters. Repeating either or
        sending any other parameter is a 400 validation_error.
        """
        query = parse_qs(urlsplit(self.path).query, keep_blank_values=True)
        unknown = [name for name in query if name not in METRICS_QUERY_PARAMETERS]
        if unknown:
            raise ValidationError(f"unknown query parameter: {sorted(unknown)[0]}")
        for name in METRICS_QUERY_PARAMETERS:
            if len(query.get(name, [])) > 1:
                raise ValidationError(f"query parameter {name} must appear at most once")
        return (
            _parse_timestamp(query.get("since", [None])[0], "since"),
            _parse_timestamp(query.get("until", [None])[0], "until"),
        )

    def _events_query(self) -> dict[str, Any]:
        """Parse the optional filter and pagination parameters of the events query.

        ``types`` is a comma-separated set of known event types without empty
        or duplicate entries; ``since`` and ``until`` are ISO-8601 UTC
        timestamps ending in Z; ``cursor`` and ``limit`` are positive
        integers. Every parameter may appear at most once, and any other
        parameter is a 400 validation_error, as is any malformed value.
        """
        query = parse_qs(urlsplit(self.path).query, keep_blank_values=True)
        unknown = [name for name in query if name not in EVENTS_QUERY_PARAMETERS]
        if unknown:
            raise ValidationError(f"unknown query parameter: {sorted(unknown)[0]}")
        for name in EVENTS_QUERY_PARAMETERS:
            if len(query.get(name, [])) > 1:
                raise ValidationError(f"query parameter {name} must appear at most once")
        parsed: dict[str, Any] = {}
        if "types" in query:
            entries = query["types"][0].split(",")
            if any(not entry for entry in entries):
                raise ValidationError("types must be a comma-separated set of event types without empty entries")
            if len(set(entries)) != len(entries):
                raise ValidationError("types must not contain duplicate event types")
            unknown_types = [entry for entry in entries if entry not in EVENT_TYPES]
            if unknown_types:
                raise ValidationError(f"unknown event type: {unknown_types[0]}")
            parsed["types"] = tuple(entries)
        if "since" in query:
            parsed["since"] = _parse_timestamp(query["since"][0], "since")
        if "until" in query:
            parsed["until"] = _parse_timestamp(query["until"][0], "until")
        for name in ("cursor", "limit"):
            if name in query:
                parsed[name] = _positive_integer(query[name][0], name)
        return parsed

    def _preview_limit(self) -> int:
        """Parse the required limit parameter of the schedule preview query.

        ``limit`` is a positive integer and the only accepted parameter, and
        it must appear exactly once. A missing, repeated, or malformed value
        — or any other parameter — is a 400 validation_error.
        """
        query = parse_qs(urlsplit(self.path).query, keep_blank_values=True)
        unknown = [name for name in query if name not in PREVIEW_QUERY_PARAMETERS]
        if unknown:
            raise ValidationError(f"unknown query parameter: {sorted(unknown)[0]}")
        if len(query.get("limit", [])) > 1:
            raise ValidationError("query parameter limit must appear at most once")
        if "limit" not in query:
            raise ValidationError("query parameter limit is required")
        return _positive_integer(query["limit"][0], "limit")

    def _list_query(self, parameters: tuple[str, ...]) -> tuple[dict[str, Any], dict[str, list[str]]]:
        """Parse the shared cursor/limit pagination of the enumeration queries.

        ``limit`` is required and a positive integer; ``cursor`` is an opaque
        identifier and may appear at most once, as may every accepted
        parameter. A missing or non-positive limit, a repeated parameter, or
        any other parameter is a 400 validation_error that writes nothing.
        Returns the parsed pagination and the raw query map so callers can
        validate their own filter values.
        """
        query = parse_qs(urlsplit(self.path).query, keep_blank_values=True)
        unknown = [name for name in query if name not in parameters]
        if unknown:
            raise ValidationError(f"unknown query parameter: {sorted(unknown)[0]}")
        for name in parameters:
            if len(query.get(name, [])) > 1:
                raise ValidationError(f"query parameter {name} must appear at most once")
        if "limit" not in query:
            raise ValidationError("query parameter limit is required")
        cursor = query["cursor"][0] if "cursor" in query else None
        if cursor is not None and not cursor:
            raise ValidationError("cursor must be a non-empty identifier")
        parsed: dict[str, Any] = {
            "cursor": cursor,
            "limit": _positive_integer(query["limit"][0], "limit"),
        }
        return parsed, query

    def _workflows_list_query(self) -> dict[str, Any]:
        return self._list_query(LIST_QUERY_PARAMETERS)[0]

    def _executions_list_query(self) -> dict[str, Any]:
        """Parse the executions list filters together with its pagination.

        ``status`` must name a lifecycle status and ``termination_reason`` a
        termination reason; an unknown value is a 400 validation_error.
        ``since`` and ``until`` are the same closed creation-time bounds the
        events and metrics queries accept; a malformed timestamp is a 400
        validation_error. A ``workflow_id`` filter is checked against the
        namespace by the service, where a missing or another tenant's workflow
        is a 404.
        """
        parsed, query = self._list_query(EXECUTIONS_QUERY_PARAMETERS)
        workflow_id = query["workflow_id"][0] if "workflow_id" in query else None
        if workflow_id is not None and not workflow_id:
            raise ValidationError("workflow_id must be a non-empty string")
        parsed["workflow_id"] = workflow_id
        status = query["status"][0] if "status" in query else None
        if status is not None and status not in EXECUTION_STATUSES:
            raise ValidationError(f"unknown execution status: {status}")
        parsed["status"] = status
        reason = query["termination_reason"][0] if "termination_reason" in query else None
        if reason is not None and reason not in TERMINATION_REASONS:
            raise ValidationError(f"unknown termination reason: {reason}")
        parsed["termination_reason"] = reason
        parsed["since"] = _parse_timestamp(query["since"][0], "since") if "since" in query else None
        parsed["until"] = _parse_timestamp(query["until"][0], "until") if "until" in query else None
        return parsed

    def _body(self) -> Any:
        content_type = self.headers.get("Content-Type", "")
        if content_type.split(";", 1)[0].strip().lower() != "application/json":
            raise ValidationError("Content-Type must be application/json")
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length < 0 or length > 1_000_000:
                raise ValueError
            parsed = json.loads(self.rfile.read(length), parse_constant=_reject_non_finite)
            _assert_finite(parsed)
            return parsed
        except (ValueError, json.JSONDecodeError) as error:
            raise ValidationError("request body must be valid JSON") from error

    def _tenant(self) -> str:
        """Resolve the tenant identifier; an absent header keeps the legacy namespace."""
        if TENANT_HEADER not in self.headers:
            return ""
        tenant = self.headers.get(TENANT_HEADER)
        if not isinstance(tenant, str) or not tenant:
            raise ValidationError(f"{TENANT_HEADER} header must be a non-empty string")
        return tenant

    @staticmethod
    def _instance_index(segment: str) -> int:
        """Parse the element index path segment; a non-integer segment is malformed."""
        digits = segment[1:] if segment.startswith("-") else segment
        if not digits.isdigit():
            raise ValidationError("instance index must be an integer")
        return int(segment)

    def _dispatch(self) -> tuple[int, Any]:
        path = urlsplit(self.path).path
        parts = [part for part in path.split("/") if part]
        if self.command == "GET" and parts == ["health"]:
            return 200, {"status": "ok"}
        if self.command in ("PUT", "POST") and parts == ["quotas"]:
            return 200, self.service.declare_quota(self._body(), self.headers.get("Idempotency-Key"), self._tenant())
        if self.command == "GET" and parts == ["quotas"]:
            return 200, self.service.get_quota(self._tenant())
        if self.command == "GET" and parts == ["usage"]:
            return 200, self.service.usage(self._tenant())
        if self.command == "GET" and parts == ["bill"]:
            return 200, self.service.bill(self._tenant())
        if self.command == "GET" and parts == ["metrics"]:
            since, until = self._metrics_window()
            return 200, self.service.metrics(self._tenant(), since, until)
        if self.command == "GET" and parts == ["metrics", "export"]:
            since, until = self._metrics_window()
            return 200, TextResponse(
                "text/plain; version=0.0.4; charset=utf-8",
                self.service.metrics_export(self._tenant(), since, until),
            )
        if self.command == "POST" and parts == ["workflows"]:
            return 201, self.service.create_workflow(self._body(), self.headers.get("Idempotency-Key"), self._tenant())
        if self.command == "GET" and parts == ["workflows"]:
            return 200, self.service.list_workflows(self._tenant(), **self._workflows_list_query())
        if len(parts) == 2 and parts[0] == "workflows" and self.command == "GET":
            return 200, self.service.get_workflow(parts[1], self._tenant())
        if len(parts) == 3 and parts[0] == "workflows" and parts[2] == "schedule" and self.command == "GET":
            return 200, self.service.schedule_status(parts[1], self._tenant())
        if len(parts) == 4 and parts[0] == "workflows" and parts[2] == "schedule" and parts[3] == "preview" and self.command == "GET":
            return 200, self.service.schedule_preview(parts[1], self._preview_limit(), self._tenant())
        if len(parts) == 3 and parts[0] == "workflows" and parts[2] == "schedule" and self.command in ("POST", "PUT"):
            return 200, self.service.update_schedule(parts[1], self._body(), self.headers.get("Idempotency-Key"), self._tenant())
        if len(parts) == 4 and parts[0] == "workflows" and parts[2] == "schedule" and parts[3] == "pause" and self.command == "POST":
            return 200, self.service.pause_schedule(parts[1], self._body(), self.headers.get("Idempotency-Key"), self._tenant())
        if len(parts) == 4 and parts[0] == "workflows" and parts[2] == "schedule" and parts[3] == "resume" and self.command == "POST":
            return 200, self.service.resume_schedule(parts[1], self._body(), self.headers.get("Idempotency-Key"), self._tenant())
        if self.command == "POST" and parts == ["executions"]:
            return 201, self.service.create_execution(self._body(), self.headers.get("Idempotency-Key"), self._tenant())
        if self.command == "GET" and parts == ["executions"]:
            return 200, self.service.list_executions(self._tenant(), **self._executions_list_query())
        if len(parts) == 2 and parts[0] == "executions" and self.command == "GET":
            return 200, self.service.get_execution(parts[1], self._tenant())
        if len(parts) == 3 and parts[0] == "executions" and parts[2] == "events" and self.command == "GET":
            return 200, {"events": self.service.events(parts[1], self._tenant(), **self._events_query())}
        if len(parts) == 3 and parts[0] == "executions" and parts[2] == "checkpoints" and self.command == "GET":
            return 200, self.service.checkpoints(parts[1], self._tenant())
        if len(parts) == 3 and parts[0] == "executions" and parts[2] == "deliveries" and self.command == "GET":
            return 200, self.service.deliveries(parts[1], self._tenant())
        if len(parts) == 3 and parts[0] == "executions" and parts[2] == "queues" and self.command == "GET":
            return 200, self.service.queues(parts[1], self._tenant())
        if len(parts) == 5 and parts[0] == "executions" and parts[2] == "queues" and parts[4] == "pull" and self.command == "POST":
            return 200, self.service.pull_queue(parts[1], parts[3], self._body(), self.headers.get("Idempotency-Key"), self._tenant())
        if len(parts) == 5 and parts[0] == "executions" and parts[2] == "queues" and parts[4] == "ack" and self.command == "POST":
            return 200, self.service.ack_queue(parts[1], parts[3], self._body(), self.headers.get("Idempotency-Key"), self._tenant())
        # "reexpand" is the public name of the re-expansion operation;
        # "expand" is kept as a documented alias. Both call the same
        # operation with identical behavior, events, and results.
        if len(parts) == 5 and parts[0] == "executions" and parts[2] == "maps" and parts[4] in ("reexpand", "expand") and self.command == "POST":
            return 200, self.service.reexpand_map(
                parts[1], parts[3], self._body(), self.headers.get("Idempotency-Key"), self._tenant()
            )
        if len(parts) == 7 and parts[0] == "executions" and parts[2] == "maps" and parts[4] == "instances" and parts[6] == "delete" and self.command == "POST":
            return 200, self.service.delete_map_instance(
                parts[1], parts[3], self._instance_index(parts[5]), self._body(), self.headers.get("Idempotency-Key"), self._tenant()
            )
        if len(parts) == 7 and parts[0] == "executions" and parts[2] == "maps" and parts[4] == "instances" and parts[6] == "modify" and self.command == "POST":
            return 200, self.service.modify_map_instance(
                parts[1], parts[3], self._instance_index(parts[5]), self._body(), self.headers.get("Idempotency-Key"), self._tenant()
            )
        if len(parts) == 3 and parts[0] == "executions" and parts[2] == "advance" and self.command == "POST":
            return 200, self.service.advance(parts[1], self._body(), self.headers.get("Idempotency-Key"), self._tenant())
        if len(parts) == 3 and parts[0] == "executions" and parts[2] == "decision" and self.command == "POST":
            return 200, self.service.decision(parts[1], self._body(), self.headers.get("Idempotency-Key"), self._tenant())
        if len(parts) == 3 and parts[0] == "executions" and parts[2] == "migrate" and self.command == "POST":
            return 200, self.service.migrate(parts[1], self._body(), self.headers.get("Idempotency-Key"), self._tenant())
        if len(parts) == 3 and parts[0] == "executions" and parts[2] == "claim" and self.command == "POST":
            return 200, self.service.claim(parts[1], self._body(), self.headers.get("Idempotency-Key"), self._tenant())
        if len(parts) == 3 and parts[0] == "executions" and parts[2] == "heartbeat" and self.command == "POST":
            return 200, self.service.heartbeat(parts[1], self._body(), self.headers.get("Idempotency-Key"), self._tenant())
        if len(parts) == 3 and parts[0] == "executions" and parts[2] == "release" and self.command == "POST":
            return 200, self.service.release(parts[1], self._body(), self.headers.get("Idempotency-Key"), self._tenant())
        if len(parts) == 3 and parts[0] == "executions" and parts[2] == "cancel" and self.command == "POST":
            return 200, self.service.cancel(parts[1], self.headers.get("Idempotency-Key"), self._tenant())
        if len(parts) == 3 and parts[0] == "executions" and parts[2] == "recover" and self.command == "POST":
            return 200, self.service.recover(parts[1], self._body(), self.headers.get("Idempotency-Key"), self._tenant())
        if len(parts) == 3 and parts[0] == "executions" and parts[2] == "replay" and self.command == "POST":
            return 200, self.service.replay(parts[1], self._tenant())
        raise NotFoundError("route was not found")

    def _handle(self) -> None:
        try:
            status, response = self._dispatch()
            self._write(status, response)
        except ChronicleFlowError as error:
            self._json(error.status, {"error": {"code": error.code, "message": str(error)}})
        except Exception:
            self._json(500, {"error": {"code": "internal_error", "message": "internal server error"}})

    do_GET = _handle
    do_POST = _handle
    do_PUT = _handle


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the ChronicleFlow HTTP service")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=8080, type=int)
    parser.add_argument("--database", default="chronicleflow.db")
    arguments = parser.parse_args()
    Handler.service = ChronicleFlow(arguments.database)
    server = ThreadingHTTPServer((arguments.host, arguments.port), Handler)
    print(f"ChronicleFlow listening on http://{arguments.host}:{arguments.port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
