from __future__ import annotations

import argparse
import json
import math
from collections import namedtuple
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlsplit

from .errors import ChronicleFlowError, NotFoundError, ValidationError
from .service import ChronicleFlow, _parse_timestamp

TENANT_HEADER = "X-Tenant-Id"

# The only query parameters the metrics query and the Prometheus export
# accept; anything else is a validation error exactly like an unknown field.
METRICS_QUERY_PARAMETERS = ("since", "until")

# A response rendered as a non-JSON text body (the Prometheus export).
TextResponse = namedtuple("TextResponse", ("content_type", "body"))


def _reject_non_finite(constant: str) -> Any:
    raise ValidationError(f"request body must not contain {constant}")


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
        if len(parts) == 2 and parts[0] == "workflows" and self.command == "GET":
            return 200, self.service.get_workflow(parts[1], self._tenant())
        if len(parts) == 3 and parts[0] == "workflows" and parts[2] == "schedule" and self.command == "GET":
            return 200, self.service.schedule_status(parts[1], self._tenant())
        if len(parts) == 3 and parts[0] == "workflows" and parts[2] == "schedule" and self.command in ("POST", "PUT"):
            return 200, self.service.update_schedule(parts[1], self._body(), self.headers.get("Idempotency-Key"), self._tenant())
        if len(parts) == 4 and parts[0] == "workflows" and parts[2] == "schedule" and parts[3] == "pause" and self.command == "POST":
            return 200, self.service.pause_schedule(parts[1], self._body(), self.headers.get("Idempotency-Key"), self._tenant())
        if len(parts) == 4 and parts[0] == "workflows" and parts[2] == "schedule" and parts[3] == "resume" and self.command == "POST":
            return 200, self.service.resume_schedule(parts[1], self._body(), self.headers.get("Idempotency-Key"), self._tenant())
        if self.command == "POST" and parts == ["executions"]:
            return 201, self.service.create_execution(self._body(), self.headers.get("Idempotency-Key"), self._tenant())
        if len(parts) == 2 and parts[0] == "executions" and self.command == "GET":
            return 200, self.service.get_execution(parts[1], self._tenant())
        if len(parts) == 3 and parts[0] == "executions" and parts[2] == "events" and self.command == "GET":
            return 200, {"events": self.service.events(parts[1], self._tenant())}
        if len(parts) == 3 and parts[0] == "executions" and parts[2] == "checkpoints" and self.command == "GET":
            return 200, self.service.checkpoints(parts[1], self._tenant())
        if len(parts) == 3 and parts[0] == "executions" and parts[2] == "deliveries" and self.command == "GET":
            return 200, self.service.deliveries(parts[1], self._tenant())
        if len(parts) == 3 and parts[0] == "executions" and parts[2] == "queues" and self.command == "GET":
            return 200, self.service.queue_status(parts[1], self._tenant())
        if (
            len(parts) == 5
            and parts[0] == "executions"
            and parts[2] == "queues"
            and parts[4] == "pull"
            and self.command == "POST"
        ):
            return 200, self.service.pull_queue(
                parts[3], self._body(), self.headers.get("Idempotency-Key"), self._tenant(), parts[1]
            )
        if (
            len(parts) == 5
            and parts[0] == "executions"
            and parts[2] == "queues"
            and parts[4] == "acknowledge"
            and self.command == "POST"
        ):
            return 200, self.service.acknowledge_queue(
                parts[3], self._body(), self.headers.get("Idempotency-Key"), self._tenant(), parts[1]
            )
        if len(parts) == 3 and parts[0] == "queues" and parts[2] == "pull" and self.command == "POST":
            return 200, self.service.pull_queue(
                parts[1], self._body(), self.headers.get("Idempotency-Key"), self._tenant()
            )
        if len(parts) == 3 and parts[0] == "queues" and parts[2] == "acknowledge" and self.command == "POST":
            return 200, self.service.acknowledge_queue(
                parts[1], self._body(), self.headers.get("Idempotency-Key"), self._tenant()
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
    service = ChronicleFlow(arguments.database)
    Handler.service = service
    server = ThreadingHTTPServer((arguments.host, arguments.port), Handler)
    print(f"ChronicleFlow listening on http://{arguments.host}:{arguments.port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        service.close()


if __name__ == "__main__":
    main()
