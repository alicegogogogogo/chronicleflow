"""JSON reading and writing that preserves the textual spelling of numbers.

Floats arrive as JSON tokens such as ``0.12345678901234567`` or ``-0.0``.
Python's shortest-round-trip serializer would emit the nearest decimal in
fewer digits (``0.12345678901234566``), which is the same binary value but a
different text. The public contract keeps the original precision and the
``-0.0`` spelling, so a parsed float carries its source lexeme and the writer
emits that lexeme verbatim. Integers, strings, booleans, and null behave
exactly as with the standard library serializer.
"""

from __future__ import annotations

import json
import math
from typing import Any


class LexicalFloat(float):
    """A finite float remembering the exact JSON text it was read from."""

    __slots__ = ("lexeme",)

    def __new__(cls, value: float, lexeme: str) -> "LexicalFloat":
        obj = float.__new__(cls, value)
        obj.lexeme = lexeme  # type: ignore[attr-defined]
        return obj


def _capture_float(lexeme: str) -> LexicalFloat:
    value = float(lexeme)
    if not math.isfinite(value):
        raise ValueError("request body must not contain non-finite numbers")
    return LexicalFloat(value, lexeme)


def _reject_constant(constant: str) -> Any:
    raise ValueError(f"request body must not contain {constant}")


def loads(text: str) -> Any:
    """Parse JSON, tagging floats with their source text.

    ``NaN``, ``Infinity``, and overflowing literals such as ``1e400`` are
    rejected, since request bodies must not contain non-finite numbers.
    """
    return json.loads(text, parse_float=_capture_float, parse_constant=_reject_constant)


def dumps(value: Any, *, sort_keys: bool = False) -> str:
    """Serialize compact JSON, preserving tagged float spellings."""
    parts: list[str] = []
    _encode(value, sort_keys, parts)
    return "".join(parts)


def _encode(value: Any, sort_keys: bool, parts: list[str]) -> None:
    if value is None:
        parts.append("null")
    elif value is True:
        parts.append("true")
    elif value is False:
        parts.append("false")
    elif isinstance(value, LexicalFloat):
        parts.append(value.lexeme)  # type: ignore[attr-defined]
    elif isinstance(value, float):
        if not math.isfinite(value):
            # Non-finite values are rejected on input, so a stored document
            # can never legitimately need NaN or Infinity on the wire.
            raise ValueError("out of range float values are not JSON compliant")
        text = repr(value)
        parts.append("-0.0" if text == "-0.0" else text)
    elif isinstance(value, int):
        parts.append(int.__repr__(value))
    elif isinstance(value, str):
        parts.append(json.dumps(value, ensure_ascii=False))
    elif isinstance(value, (list, tuple)):
        parts.append("[")
        for index, item in enumerate(value):
            if index:
                parts.append(",")
            _encode(item, sort_keys, parts)
        parts.append("]")
    elif isinstance(value, dict):
        items = sorted(value.items(), key=lambda item: item[0]) if sort_keys else value.items()
        parts.append("{")
        for index, (key, item) in enumerate(items):
            if index:
                parts.append(",")
            if not isinstance(key, str):
                raise TypeError(f"keys must be str, not {type(key).__name__}")
            parts.append(json.dumps(key, ensure_ascii=False))
            parts.append(":")
            _encode(item, sort_keys, parts)
        parts.append("}")
    else:
        raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")
