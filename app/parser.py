from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

LOGGER = logging.getLogger(__name__)

SML_START = b"\x1b\x1b\x1b\x1b\x01\x01\x01\x01"

# OBIS identifiers from the SmartCircuits DTZ541 script.
OBIS_1_8_0 = bytes.fromhex("0100010800ff")
OBIS_2_8_0 = bytes.fromhex("0100020800ff")
OBIS_16_7_0 = bytes.fromhex("0100100700ff")
OBIS_96_1_0 = bytes.fromhex("0100600100ff")

OBIS_MAP: dict[str, bytes] = {
    "energy_import_kwh": OBIS_1_8_0,
    "energy_export_kwh": OBIS_2_8_0,
    "power_w": OBIS_16_7_0,
    "server_id": OBIS_96_1_0,
}

DIVISORS: dict[str, float] = {
    "energy_import_kwh": 1000.0,
    "energy_export_kwh": 1000.0,
    "power_w": 1.0,
}


@dataclass
class FrameResult:
    values: dict[str, Any]


class FrameStreamExtractor:
    """
    Splits a continuous SML byte stream into likely telegram-sized frames.

    For this meter stream, a pragmatic approach works well:
    whenever two start markers are present, the bytes in between are treated
    as one complete telegram.
    """

    def __init__(self) -> None:
        self._buffer = bytearray()

    def feed(self, chunk: bytes) -> list[bytes]:
        self._buffer.extend(chunk)
        frames: list[bytes] = []

        while True:
            first = self._find_start(0)
            if first < 0:
                if len(self._buffer) > 8192:
                    self._buffer = self._buffer[-4096:]
                return frames

            if first > 0:
                del self._buffer[:first]

            second = self._find_start(len(SML_START))
            if second < 0:
                return frames

            frame = bytes(self._buffer[:second])
            frames.append(frame)
            del self._buffer[:second]

    def _find_start(self, offset: int) -> int:
        return bytes(self._buffer).find(SML_START, offset)


def parse_frame(frame: bytes) -> FrameResult:
    values: dict[str, Any] = {}

    for key, obis_bytes in OBIS_MAP.items():
        segment = _find_obis_segment(frame, obis_bytes)
        if segment is None:
            continue

        if key == "server_id":
            parsed = _extract_server_id(segment)
        else:
            parsed = _extract_numeric(segment, key)

        if parsed is not None:
            values[key] = parsed

    return FrameResult(values=values)


def _find_obis_segment(frame: bytes, obis_bytes: bytes) -> bytes | None:
    needle = b"\x77\x07" + obis_bytes
    start = frame.find(needle)
    if start < 0:
        return None

    next_marker = frame.find(b"\x77\x07", start + len(needle))
    if next_marker < 0:
        return frame[start:]
    return frame[start:next_marker]


def _extract_numeric(segment: bytes, key: str) -> float | int | None:
    """
    Extract a numeric value from a pragmatic subset of SML-like field encodings.

    We look for patterns such as:
      62 <unit> 52 <scaler> 65 <4-byte-unsigned>
    which appear in the Holley sample stream.
    """
    candidates: list[tuple[int, int]] = []

    i = 0
    while i < len(segment) - 2:
        marker = segment[i]
        if marker in _NUMERIC_LENGTHS:
            value_len, signed = _NUMERIC_LENGTHS[marker]
            end = i + 1 + value_len
            if end <= len(segment):
                raw = segment[i + 1 : end]
                parsed = int.from_bytes(raw, byteorder="big", signed=signed)
                candidates.append((i, parsed))
                i = end
                continue
        i += 1

    if not candidates:
        return None

    # Use the last numeric candidate in the segment; in the observed Holley
    # payloads, the actual reading appears at the end of the OBIS block.
    _, raw_value = candidates[-1]

    divisor = DIVISORS.get(key, 1.0)
    value = raw_value / divisor

    if divisor == 1.0:
        return int(value)
    return round(value, 3)


def _extract_server_id(segment: bytes) -> str | None:
    """
    Extract a readable meter/server identifier from the OBIS 96.1.0 segment.

    This is intentionally pragmatic:
    we collect printable ASCII runs and prefer the longest useful one.
    """
    runs: list[str] = []
    current: list[str] = []

    for byte in segment:
        if 32 <= byte <= 126:
            current.append(chr(byte))
        else:
            if len(current) >= 3:
                runs.append("".join(current))
            current = []

    if len(current) >= 3:
        runs.append("".join(current))

    if not runs:
        return None

    # Prefer a reasonably compact ID-like token.
    runs.sort(key=len, reverse=True)
    best = runs[0].strip()

    # Very long runs are unlikely to be the meter ID itself.
    if len(best) > 32:
        best
