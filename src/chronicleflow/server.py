from __future__ import annotations

import argparse
import json
import math
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlsplit

from .errors import ChronicleFlowError, NotFoundError, ValidationError
from .service import ChronicleFlow


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

    def _dispatch(self) -> tuple[int, Any]:
        path = urlsplit(self.path).path
        parts = [part for part in path.split("/") if part]
        if self.command == "GET" and parts == ["health"]:
            return 200, {"status": "ok"}
        if self.command == "POST" and parts == ["workflows"]:
            return 201, self.service.create_workflow(self._body(), self.headers.get("Idempotency-Key"))
        if len(parts) == 3 and parts[0] == "workflows" and parts[2] == "schedule" and self.command == "GET":
            return 200, self.service.schedule_status(parts[1])
        if len(parts) == 3 and parts[0] == "workflows" and parts[2] == "schedule" and self.command in ("POST", "PUT"):
            return 200, self.service.update_schedule(parts[1], self._body(), self.headers.get("Idempotency-Key"))
        if len(parts) == 4 and parts[0] == "workflows" and parts[2] == "schedule" and parts[3] == "pause" and self.command == "POST":
            return 200, self.service.pause_schedule(parts[1], self._body(), self.headers.get("Idempotency-Key"))
        if len(parts) == 4 and parts[0] == "workflows" and parts[2] == "schedule" and parts[3] == "resume" and self.command == "POST":
            return 200, self.service.resume_schedule(parts[1], self._body(), self.headers.get("Idempotency-Key"))
        if self.command == "POST" and parts == ["executions"]:
            return 201, self.service.create_execution(self._body(), self.headers.get("Idempotency-Key"))
        if len(parts) == 2 and parts[0] == "executions" and self.command == "GET":
            return 200, self.service.get_execution(parts[1])
        if len(parts) == 3 and parts[0] == "executions" and parts[2] == "events" and self.command == "GET":
            return 200, {"events": self.service.events(parts[1])}
        if len(parts) == 3 and parts[0] == "executions" and parts[2] == "checkpoints" and self.command == "GET":
            return 200, self.service.checkpoints(parts[1])
        if len(parts) == 3 and parts[0] == "executions" and parts[2] == "deliveries" and self.command == "GET":
            return 200, self.service.deliveries(parts[1])
        if len(parts) == 3 and parts[0] == "executions" and parts[2] == "advance" and self.command == "POST":
            return 200, self.service.advance(parts[1], self._body(), self.headers.get("Idempotency-Key"))
        if len(parts) == 3 and parts[0] == "executions" and parts[2] == "decision" and self.command == "POST":
            return 200, self.service.decision(parts[1], self._body(), self.headers.get("Idempotency-Key"))
        if len(parts) == 3 and parts[0] == "executions" and parts[2] == "claim" and self.command == "POST":
            return 200, self.service.claim(parts[1], self._body(), self.headers.get("Idempotency-Key"))
        if len(parts) == 3 and parts[0] == "executions" and parts[2] == "heartbeat" and self.command == "POST":
            return 200, self.service.heartbeat(parts[1], self._body(), self.headers.get("Idempotency-Key"))
        if len(parts) == 3 and parts[0] == "executions" and parts[2] == "release" and self.command == "POST":
            return 200, self.service.release(parts[1], self._body(), self.headers.get("Idempotency-Key"))
        if len(parts) == 3 and parts[0] == "executions" and parts[2] == "cancel" and self.command == "POST":
            return 200, self.service.cancel(parts[1], self.headers.get("Idempotency-Key"))
        if len(parts) == 3 and parts[0] == "executions" and parts[2] == "recover" and self.command == "POST":
            return 200, self.service.recover(parts[1], self._body(), self.headers.get("Idempotency-Key"))
        if len(parts) == 3 and parts[0] == "executions" and parts[2] == "replay" and self.command == "POST":
            return 200, self.service.replay(parts[1])
        raise NotFoundError("route was not found")

    def _handle(self) -> None:
        try:
            status, response = self._dispatch()
            self._json(status, response)
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

