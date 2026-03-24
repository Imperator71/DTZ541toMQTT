from __future__ import annotations

from collections import deque
from dataclasses import dataclass
import logging
import os
import socket
from statistics import median
import time
from typing import Any

import serial

from app.config import load_settings
from app.discovery import publish_homeassistant_discovery
from app.mqtt_publish import MqttPublisher
from app.parser import FrameStreamExtractor, parse_frame


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


LOGGER = logging.getLogger(__name__)

ENERGY_COUNTER_KEYS = {"energy_import_kwh", "energy_export_kwh"}


@dataclass
class EnergySample:
    value: float
    timestamp: float


@dataclass
class PendingResetCandidate:
    first_value: float
    last_value: float
    first_seen_at: float
    seen_count: int = 1


@dataclass
class EnergyValidationState:
    history: deque[EnergySample]
    bootstrap_candidate: EnergySample | None = None
    pending_reset: PendingResetCandidate | None = None


def _build_energy_validation_state(window_size: int) -> dict[str, EnergyValidationState]:
    return {
        key: EnergyValidationState(history=deque(maxlen=window_size))
        for key in ENERGY_COUNTER_KEYS
    }


def _clear_energy_validation_state(states: dict[str, EnergyValidationState]) -> None:
    for state in states.values():
        state.history.clear()
        state.bootstrap_candidate = None
        state.pending_reset = None


def _format_frame_hex(frame: bytes, *, max_bytes: int = 512) -> str:
    if len(frame) <= max_bytes:
        return frame.hex()
    truncated = frame[:max_bytes]
    return f"{truncated.hex()}...(+{len(frame) - max_bytes} bytes)"


def _publish_faulty_read(
    mqtt: MqttPublisher,
    settings: Any,
    payload: dict[str, Any],
) -> None:
    if not settings.publish_faulty_reads:
        return

    topic = f"{settings.mqtt_topic_prefix}/{settings.faulty_read_topic_suffix}"
    mqtt.publish_value(topic, payload, retain=False)


def _allowed_energy_delta_kwh(elapsed_seconds: float, settings: Any) -> float:
    return max(0.0, settings.energy_max_delta_kwh_per_hour * elapsed_seconds / 3600.0)


def _validate_energy_value(
    key: str,
    value: Any,
    now: float,
    state: EnergyValidationState,
    settings: Any,
    mqtt: MqttPublisher,
) -> float | None:
    if not isinstance(value, (int, float)):
        return None

    candidate_value = float(value)

    if not state.history:
        bootstrap = state.bootstrap_candidate
        sample = EnergySample(value=candidate_value, timestamp=now)
        if bootstrap is None:
            state.bootstrap_candidate = sample
            return None

        elapsed = max(now - bootstrap.timestamp, 0.0)
        bootstrap_delta = _allowed_energy_delta_kwh(elapsed, settings)
        if (
            candidate_value >= bootstrap.value - settings.energy_negative_jitter_kwh
            and abs(candidate_value - bootstrap.value)
            <= bootstrap_delta + settings.energy_negative_jitter_kwh
        ):
            state.history.append(bootstrap)
            state.history.append(sample)
            state.bootstrap_candidate = None
            return candidate_value

        _publish_faulty_read(
            mqtt,
            settings,
            {
                "key": key,
                "reason": "bootstrap_rejected",
                "candidate": round(bootstrap.value, 3),
                "replacement": round(candidate_value, 3),
            },
        )
        state.bootstrap_candidate = sample
        return None

    latest = state.history[-1]
    median_value = median(sample.value for sample in state.history)
    elapsed = max(now - latest.timestamp, 0.0)
    allowed_forward_delta = _allowed_energy_delta_kwh(elapsed, settings)
    negative_limit = latest.value - settings.energy_negative_jitter_kwh
    positive_limit = latest.value + allowed_forward_delta

    is_within_limits = negative_limit <= candidate_value <= positive_limit
    is_near_median = candidate_value >= median_value - settings.energy_negative_jitter_kwh

    if is_within_limits and is_near_median:
        state.pending_reset = None
        state.history.append(EnergySample(value=candidate_value, timestamp=now))
        return candidate_value

    if candidate_value > positive_limit:
        state.pending_reset = None
        _publish_faulty_read(
            mqtt,
            settings,
            {
                "key": key,
                "reason": "positive_outlier",
                "candidate": round(candidate_value, 3),
                "last": round(latest.value, 3),
                "median": round(median_value, 3),
                "allowed_delta_kwh": round(allowed_forward_delta, 6),
                "actual_delta_kwh": round(candidate_value - latest.value, 3),
            },
        )
        return None

    pending = state.pending_reset
    if pending is None:
        state.pending_reset = PendingResetCandidate(
            first_value=candidate_value,
            last_value=candidate_value,
            first_seen_at=now,
        )
        _publish_faulty_read(
            mqtt,
            settings,
            {
                "key": key,
                "reason": "negative_outlier_pending",
                "candidate": round(candidate_value, 3),
                "last": round(latest.value, 3),
                "median": round(median_value, 3),
                "actual_delta_kwh": round(candidate_value - latest.value, 3),
            },
        )
        return None

    allowed_pending_growth = _allowed_energy_delta_kwh(max(now - pending.first_seen_at, 0.0), settings)
    if candidate_value <= negative_limit and candidate_value <= pending.last_value + allowed_pending_growth:
        pending.last_value = candidate_value
        pending.seen_count += 1
    else:
        state.pending_reset = PendingResetCandidate(
            first_value=candidate_value,
            last_value=candidate_value,
            first_seen_at=now,
        )
        return None

    if state.pending_reset.seen_count < settings.energy_reset_confirm_frames:
        return None

    LOGGER.warning(
        "Accepted rebased %s after %s confirming frames: %s -> %s",
        key,
        state.pending_reset.seen_count,
        latest.value,
        candidate_value,
    )
    _publish_faulty_read(
        mqtt,
        settings,
        {
            "key": key,
            "reason": "negative_rebase_accepted",
            "candidate": round(candidate_value, 3),
            "last": round(latest.value, 3),
            "median": round(median_value, 3),
            "confirm_frames": state.pending_reset.seen_count,
        },
    )
    state.history.clear()
    state.history.append(EnergySample(value=candidate_value, timestamp=now))
    state.pending_reset = None
    return candidate_value


def _filter_values(
    values: dict[str, Any],
    now: float,
    settings: Any,
    mqtt: MqttPublisher,
    energy_states: dict[str, EnergyValidationState],
) -> dict[str, Any]:
    filtered: dict[str, Any] = {}
    for key, value in values.items():
        if key not in ENERGY_COUNTER_KEYS:
            filtered[key] = value
            continue

        validated_value = _validate_energy_value(
            key,
            value,
            now,
            energy_states[key],
            settings,
            mqtt,
        )
        if validated_value is not None:
            filtered[key] = validated_value

    return filtered


def _process_frame(
    frame: bytes,
    settings: Any,
    mqtt: MqttPublisher,
    last_published: dict[str, Any],
    discovery_published: bool,
    last_publish_time: float,
    energy_states: dict[str, EnergyValidationState],
    last_meter_id: str | None,
) -> tuple[bool, float, str | None]:
    if settings.log_raw_frames:
        LOGGER.info(
            "Raw frame (%s bytes): %s",
            len(frame),
            _format_frame_hex(frame),
        )

    result = parse_frame(frame)
    if settings.log_parsed_frames:
        LOGGER.info("Parsed frame: %s", result.values or {})
    if not result.values:
        return discovery_published, last_publish_time, last_meter_id

    meter_id = result.values.get("server_id")
    if isinstance(meter_id, str) and last_meter_id and meter_id != last_meter_id:
        LOGGER.warning("Meter id changed from %s to %s, resetting energy validation state", last_meter_id, meter_id)
        _clear_energy_validation_state(energy_states)
    if isinstance(meter_id, str):
        last_meter_id = meter_id

    now = time.time()
    filtered_values = _filter_values(result.values, now, settings, mqtt, energy_states)
    if not filtered_values:
        return discovery_published, last_publish_time, last_meter_id

    if settings.mqtt_discovery and not discovery_published:
        publish_homeassistant_discovery(
            mqtt=mqtt,
            settings=settings,
            meter_id=meter_id if isinstance(meter_id, str) else None,
        )
        discovery_published = True

    should_publish = now - last_publish_time >= settings.publish_interval_seconds
    if should_publish:
        for key, value in filtered_values.items():
            if last_published.get(key) != value:
                mqtt.publish_state(key, value, retain=True)
                LOGGER.info("Published %s=%s", key, value)
                last_published[key] = value
        last_publish_time = now

    return discovery_published, last_publish_time, last_meter_id


def main() -> None:
    settings = load_settings()
    configure_logging(settings.log_level)
    LOGGER.info("DTZ541toMQTT starting, version=%s", os.getenv("APP_VERSION", "dev"))

    mqtt = MqttPublisher(settings)
    mqtt.connect()

    discovery_published = False
    last_published: dict[str, Any] = {}
    last_publish_time = 0.0
    last_meter_id: str | None = None
    energy_states = _build_energy_validation_state(settings.energy_validation_window_size)

    extractor = FrameStreamExtractor()
    backoff_seconds = 1.0
    max_backoff_seconds = 30.0

    if settings.serial_device:
        LOGGER.info(
            "Reading meter stream from serial device %s at %sbps",
            settings.serial_device,
            settings.serial_baud,
        )
    else:
        LOGGER.info(
            "Reading meter stream from TCP %s:%s",
            settings.tcp_host,
            settings.tcp_port,
        )

    while True:
        try:
            extractor.reset()
            if settings.serial_device:
                with serial.Serial(
                    settings.serial_device,
                    settings.serial_baud,
                    timeout=settings.serial_timeout_seconds,
                ) as ser:
                    LOGGER.info("Serial port opened")
                    backoff_seconds = 1.0
                    while True:
                        chunk = ser.read(4096)
                        if not chunk:
                            continue

                        for frame in extractor.feed(chunk):
                            discovery_published, last_publish_time, last_meter_id = _process_frame(
                                frame,
                                settings,
                                mqtt,
                                last_published,
                                discovery_published,
                                last_publish_time,
                                energy_states,
                                last_meter_id,
                            )
            else:
                LOGGER.info(
                    "Connecting to meter stream at %s:%s",
                    settings.tcp_host,
                    settings.tcp_port,
                )

                with socket.create_connection(
                    (settings.tcp_host, settings.tcp_port),
                    timeout=settings.socket_timeout_seconds,
                ) as sock:
                    sock.settimeout(settings.socket_timeout_seconds)
                    LOGGER.info("Connected to meter stream")
                    backoff_seconds = 1.0

                    while True:
                        chunk = sock.recv(4096)
                        if not chunk:
                            raise ConnectionError("Meter stream closed the connection")

                        for frame in extractor.feed(chunk):
                            discovery_published, last_publish_time, last_meter_id = _process_frame(
                                frame,
                                settings,
                                mqtt,
                                last_published,
                                discovery_published,
                                last_publish_time,
                                energy_states,
                                last_meter_id,
                            )

        except Exception as exc:
            LOGGER.exception("Bridge loop failed: %s", exc)
            LOGGER.info("Retrying in %.1f seconds", backoff_seconds)
            time.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2, max_backoff_seconds)


if __name__ == "__main__":
    main()
