from __future__ import annotations

from dataclasses import dataclass
from typing import Any

SML_START = b"\x1b\x1b\x1b\x1b\x01\x01\x01\x01"

# OBIS identifiers from the SmartCircuits DTZ541 script.
OBIS_MAP: dict[str, bytes] = {
    "energy_import_kwh": bytes.fromhex("0100010800ff"),  # 1.8.0
    "energy_export_kwh": bytes.fromhex("0100020800ff"),  # 2.8.0
    "power_w": bytes.fromhex("0100100700ff"),           # 16.7.0
    "frequency_hz": bytes.fromhex("01000e0700ff"),      # 14.7.0
    "current_l1_a": bytes.fromhex("01001f0700ff"),      # 31.7.0
    "voltage_l1_v": bytes.fromhex("0100200700ff"),      # 32.7.0
    "current_l2_a": bytes.fromhex("0100330700ff"),      # 51.7.0
    "voltage_l2_v": bytes.fromhex("0100340700ff"),      # 52.7.0
    "current_l3_a": bytes.fromhex("0100470700ff"),      # 71.7.0
    "voltage_l3_v": bytes.fromhex("0100480700ff"),      # 72.7.0
    "server_id": bytes.fromhex("0100600100ff"),         # 96.1.0
}

# Default scaling when no explicit scaler is present or usable.
DIVISORS: dict[str, float] = {
    "energy_import_kwh": 1000.0,
    "energy_export_kwh": 1000.0,
    "power_w": 1.0,
    "frequency_hz": 1.0,
    "current_l1_a": 1.0,
    "current_l2_a": 1.0,
    "current_l3_a": 1.0,
    "voltage_l1_v": 1.0,
    "voltage_l2_v": 1.0,
    "voltage_l3_v": 1.0,
}

_NUMERIC_LENGTHS: dict[int, tuple[int, bool]] = {
    0x52: (1, True),
    0x53: (2, True),
    0x54: (3, True),
    0x55: (4, True),
    0x56: (5, True),
    0x57: (6, True),
    0x62: (1, False),
    0x63: (2, False),
    0x64: (3, False),
    0x65: (4, False),
    0x66: (5, False),
    0x67: (6, False),
}


@dataclass
class FrameResult:
    values: dict[str, Any]


class FrameStreamExtractor:
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
        return self._buffer.find(SML_START, offset)


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
    scaler = _extract_scaler(segment)
    candidates = _extract_numeric_candidates(segment)
    if not candidates:
        return None

    raw_value = _pick_best_numeric(candidates, key)
    value = float(raw_value)

    if scaler is not None:
        value *= 10 ** scaler
    else:
        value /= DIVISORS.get(key, 1.0)

    if key in {"power_w"}:
        return int(round(value))

    return round(value, 3)


def _extract_scaler(segment: bytes) -> int | None:
    """
    Look for signed 1-byte scaler markers like:
      52 ff  -> -1
      52 00  -> 0
      52 01  -> 1
    Prefer the last scaler seen in the segment.
    """
    scaler: int | None = None
    for i in range(len(segment) - 1):
        if segment[i] == 0x52:
            raw = segment[i + 1 : i + 2]
            scaler = int.from_bytes(raw, byteorder="big", signed=True)
    return scaler


def _extract_numeric_candidates(segment: bytes) -> list[tuple[int, int, bool]]:
    """
    Return tuples of (offset, parsed_value, signed).
    """
    candidates: list[tuple[int, int, bool]] = []
    i = 0
    while i < len(segment) - 1:
        marker = segment[i]
        if marker in _NUMERIC_LENGTHS:
            value_len, signed = _NUMERIC_LENGTHS[marker]
            end = i + 1 + value_len
            if end <= len(segment):
                raw = segment[i + 1 : end]
                parsed = int.from_bytes(raw, byteorder="big", signed=signed)
                candidates.append((i, parsed, signed))
                i = end
                continue
        i += 1
    return candidates


def _pick_best_numeric(
    candidates: list[tuple[int, int, bool]],
    key: str,
) -> int:
    """
    Heuristic:
    - prefer the last unsigned value for counters/voltages/currents/frequency
    - for power, prefer the last non-zero signed/unsigned value that looks realistic
    """
    if key == "power_w":
        realistic: list[int] = []
        for _, value, _ in candidates:
            if abs(value) <= 200_000:
                realistic.append(value)
        if realistic:
            # prefer the last non-zero reading, else the last realistic one
            non_zero = [v for v in realistic if v != 0]
            return non_zero[-1] if non_zero else realistic[-1]

    # For all other measurements, use the last positive candidate.
    positives = [value for _, value, _ in candidates if value >= 0]
    if positives:
        return positives[-1]

    return candidates[-1][1]


def _extract_server_id(segment: bytes) -> str | None:
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

    # Prefer something that is not just the vendor prefix.
    filtered = [r.strip() for r in runs if r.strip() and r.strip() != "HLY"]
    if filtered:
        filtered.sort(key=len, reverse=True)
        best = filtered[0]
    else:
        runs.sort(key=len, reverse=True)
        best = runs[0].strip()

    if len(best) > 32:
        best = best[:32]

    return best or None
