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
    "phase_angle_l2_l1_deg": bytes.fromhex("0100510701ff"),  # 81.7.1
    "phase_angle_l3_l1_deg": bytes.fromhex("0100510702ff"),  # 81.7.2
    "phase_angle_p1_deg": bytes.fromhex("0100510704ff"),     # 81.7.4
    "phase_angle_p2_deg": bytes.fromhex("010051070fff"),     # 81.7.15
    "phase_angle_p3_deg": bytes.fromhex("010051071aff"),     # 81.7.26
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
    "phase_angle_l2_l1_deg": 1.0,
    "phase_angle_l3_l1_deg": 1.0,
    "phase_angle_p1_deg": 1.0,
    "phase_angle_p2_deg": 1.0,
    "phase_angle_p3_deg": 1.0,
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

_MIN_VALUE_LENGTH: dict[str, int] = {
    "energy_import_kwh": 4,
    "energy_export_kwh": 4,
    "power_w": 2,
    "frequency_hz": 2,
    "current_l1_a": 2,
    "current_l2_a": 2,
    "current_l3_a": 2,
    "voltage_l1_v": 2,
    "voltage_l2_v": 2,
    "voltage_l3_v": 2,
    "phase_angle_l2_l1_deg": 2,
    "phase_angle_l3_l1_deg": 2,
    "phase_angle_p1_deg": 2,
    "phase_angle_p2_deg": 2,
    "phase_angle_p3_deg": 2,
}

_VALUE_RANGES: dict[str, tuple[float, float]] = {
    "power_w": (-200_000.0, 200_000.0),
    "frequency_hz": (45.0, 65.0),
    "current_l1_a": (0.0, 200.0),
    "current_l2_a": (0.0, 200.0),
    "current_l3_a": (0.0, 200.0),
    "voltage_l1_v": (80.0, 320.0),
    "voltage_l2_v": (80.0, 320.0),
    "voltage_l3_v": (80.0, 320.0),
    "phase_angle_l2_l1_deg": (-180.0, 180.0),
    "phase_angle_l3_l1_deg": (-180.0, 180.0),
    "phase_angle_p1_deg": (-180.0, 180.0),
    "phase_angle_p2_deg": (-180.0, 180.0),
    "phase_angle_p3_deg": (-180.0, 180.0),
}


@dataclass
class FrameResult:
    values: dict[str, Any]


@dataclass(frozen=True)
class NumericCandidate:
    offset: int
    value: int
    signed: bool
    length: int
    scaler: int | None


class FrameStreamExtractor:
    def __init__(self) -> None:
        self._buffer = bytearray()

    def reset(self) -> None:
        self._buffer.clear()

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
    candidates = _extract_numeric_candidates(segment)
    if not candidates:
        return None

    candidate = _extract_unit_scaled_value(segment) or _pick_best_numeric(candidates, key)
    value = float(candidate.value)

    if candidate.scaler is not None:
        value *= 10 ** candidate.scaler
    value /= DIVISORS.get(key, 1.0)

    if key in _VALUE_RANGES:
        min_value, max_value = _VALUE_RANGES[key]
        if value < min_value or value > max_value:
            return None

    if key in {"power_w"}:
        return int(round(value))

    return round(value, 3)


def _extract_unit_scaled_value(segment: bytes) -> NumericCandidate | None:
    """
    Prefer values that follow the unit+scaler+value pattern used in SML list entries.
    This avoids picking up DTZ541 valTime quirks where a raw uint32 appears before unit.
    """
    last: NumericCandidate | None = None
    i = 0
    while i + 4 < len(segment):
        if segment[i] == 0x62 and segment[i + 2] == 0x52:
            scaler_raw = segment[i + 3 : i + 4]
            scaler = int.from_bytes(scaler_raw, byteorder="big", signed=True)
            value_marker = segment[i + 4]
            if value_marker in _NUMERIC_LENGTHS:
                value_len, signed = _NUMERIC_LENGTHS[value_marker]
                value_start = i + 5
                value_end = value_start + value_len
                if value_end <= len(segment):
                    raw = segment[value_start:value_end]
                    parsed = int.from_bytes(raw, byteorder="big", signed=signed)
                    last = NumericCandidate(
                        offset=i + 4,
                        value=parsed,
                        signed=signed,
                        length=value_len,
                        scaler=scaler,
                    )
                    i = value_end
                    continue
        i += 1
    return last


def _extract_numeric_candidates(segment: bytes) -> list[NumericCandidate]:
    candidates: list[NumericCandidate] = []
    i = 0
    while i < len(segment) - 1:
        marker = segment[i]
        if marker in _NUMERIC_LENGTHS:
            value_len, signed = _NUMERIC_LENGTHS[marker]
            end = i + 1 + value_len
            if end <= len(segment):
                raw = segment[i + 1 : end]
                parsed = int.from_bytes(raw, byteorder="big", signed=signed)
                scaler = None
                if i >= 2 and segment[i - 2] == 0x52:
                    scaler_raw = segment[i - 1 : i]
                    scaler = int.from_bytes(scaler_raw, byteorder="big", signed=True)
                candidates.append(
                    NumericCandidate(
                        offset=i,
                        value=parsed,
                        signed=signed,
                        length=value_len,
                        scaler=scaler,
                    )
                )
                i = end
                continue
        i += 1
    return candidates


def _pick_best_numeric(candidates: list[NumericCandidate], key: str) -> NumericCandidate:
    min_len = _MIN_VALUE_LENGTH.get(key, 1)
    eligible = [candidate for candidate in candidates if candidate.length >= min_len]
    if not eligible:
        eligible = candidates

    with_scaler = [candidate for candidate in eligible if candidate.scaler is not None]
    if with_scaler:
        eligible = with_scaler

    if key == "power_w":
        realistic = [c for c in eligible if abs(c.value) <= 200_000]
        if realistic:
            non_zero = [c for c in realistic if c.value != 0]
            return non_zero[-1] if non_zero else realistic[-1]

    if key in {"energy_import_kwh", "energy_export_kwh"}:
        unsigned = [c for c in eligible if not c.signed]
        if unsigned:
            max_len = max(c.length for c in unsigned)
            longest = [c for c in unsigned if c.length == max_len]
            positives = [c for c in longest if c.value >= 0]
            return positives[-1] if positives else longest[-1]

    unsigned = [c for c in eligible if not c.signed]
    if unsigned:
        positives = [c for c in unsigned if c.value >= 0]
        return positives[-1] if positives else unsigned[-1]

    positives = [c for c in eligible if c.value >= 0]
    if positives:
        return positives[-1]

    return eligible[-1]


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
