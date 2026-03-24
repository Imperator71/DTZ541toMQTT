from __future__ import annotations

import os
from dataclasses import dataclass


def _get_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _get_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"Environment variable {name} must be an integer") from exc


def _get_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    try:
        return float(value)
    except ValueError as exc:
        raise ValueError(f"Environment variable {name} must be a number") from exc


def _get_str(name: str, default: str = "") -> str:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip()


@dataclass(frozen=True)
class Settings:
    tcp_host: str
    tcp_port: int
    serial_device: str
    serial_baud: int
    serial_timeout_seconds: int

    mqtt_host: str
    mqtt_port: int
    mqtt_username: str
    mqtt_password: str
    mqtt_topic_prefix: str
    mqtt_client_id: str

    mqtt_discovery: bool
    mqtt_discovery_prefix: str

    log_level: str
    log_raw_frames: bool
    log_parsed_frames: bool
    publish_interval_seconds: int
    socket_timeout_seconds: int
    energy_validation_window_size: int
    energy_max_delta_kwh_per_hour: float
    energy_negative_jitter_kwh: float
    energy_reset_confirm_frames: int
    publish_faulty_reads: bool
    faulty_read_topic_suffix: str


def load_settings() -> Settings:
    serial_device = _get_str("SERIAL_DEVICE")

    tcp_host = _get_str("TCP_HOST")
    if not serial_device and not tcp_host:
        raise ValueError("Environment variable TCP_HOST is required when SERIAL_DEVICE is not set")

    mqtt_host = _get_str("MQTT_HOST")
    if not mqtt_host:
        raise ValueError("Environment variable MQTT_HOST is required")

    topic_prefix = _get_str("MQTT_TOPIC_PREFIX", "dtz541").strip("/")
    if not topic_prefix:
        topic_prefix = "dtz541"

    client_id = _get_str("MQTT_CLIENT_ID", "dtz541tomqtt")

    return Settings(
        tcp_host=tcp_host,
        tcp_port=_get_int("TCP_PORT", 2001),
        serial_device=serial_device,
        serial_baud=_get_int("SERIAL_BAUD", 9600),
        serial_timeout_seconds=_get_int("SERIAL_TIMEOUT_SECONDS", 2),
        mqtt_host=mqtt_host,
        mqtt_port=_get_int("MQTT_PORT", 1883),
        mqtt_username=_get_str("MQTT_USERNAME", ""),
        mqtt_password=_get_str("MQTT_PASSWORD", ""),
        mqtt_topic_prefix=topic_prefix,
        mqtt_client_id=client_id,
        mqtt_discovery=_get_bool("MQTT_DISCOVERY", True),
        mqtt_discovery_prefix=_get_str("MQTT_DISCOVERY_PREFIX", "homeassistant"),
        log_level=_get_str("LOG_LEVEL", "INFO").upper(),
        log_raw_frames=_get_bool("LOG_RAW_FRAMES", False),
        log_parsed_frames=_get_bool("LOG_PARSED_FRAMES", False),
        publish_interval_seconds=_get_int("PUBLISH_INTERVAL_SECONDS", 5),
        socket_timeout_seconds=_get_int("SOCKET_TIMEOUT_SECONDS", 15),
        energy_validation_window_size=_get_int("ENERGY_VALIDATION_WINDOW_SIZE", 5),
        energy_max_delta_kwh_per_hour=_get_float("ENERGY_MAX_DELTA_KWH_PER_HOUR", 50.0),
        energy_negative_jitter_kwh=_get_float("ENERGY_NEGATIVE_JITTER_KWH", 0.002),
        energy_reset_confirm_frames=_get_int("ENERGY_RESET_CONFIRM_FRAMES", 3),
        publish_faulty_reads=_get_bool("PUBLISH_FAULTY_READS", False),
        faulty_read_topic_suffix=_get_str("FAULTY_READ_TOPIC_SUFFIX", "faulty_read").strip("/"),
    )
