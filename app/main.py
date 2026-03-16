from __future__ import annotations

import logging
import os
import socket
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


def _format_frame_hex(frame: bytes, *, max_bytes: int = 512) -> str:
    if len(frame) <= max_bytes:
        return frame.hex()
    truncated = frame[:max_bytes]
    return f"{truncated.hex()}...(+{len(frame) - max_bytes} bytes)"


def _process_frame(
    frame: bytes,
    settings: Any,
    mqtt: MqttPublisher,
    last_published: dict[str, Any],
    discovery_published: bool,
    last_publish_time: float,
) -> tuple[bool, float]:
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
        return discovery_published, last_publish_time

    meter_id = result.values.get("server_id")
    if settings.mqtt_discovery and not discovery_published:
        publish_homeassistant_discovery(
            mqtt=mqtt,
            settings=settings,
            meter_id=meter_id if isinstance(meter_id, str) else None,
        )
        discovery_published = True

    now = time.time()
    should_publish = now - last_publish_time >= settings.publish_interval_seconds
    if should_publish:
        for key, value in result.values.items():
            if last_published.get(key) != value:
                mqtt.publish_state(key, value, retain=True)
                LOGGER.info("Published %s=%s", key, value)
                last_published[key] = value
        last_publish_time = now

    return discovery_published, last_publish_time


def main() -> None:
    settings = load_settings()
    configure_logging(settings.log_level)
    LOGGER.info("DTZ541toMQTT starting, version=%s", os.getenv("APP_VERSION", "dev"))

    mqtt = MqttPublisher(settings)
    mqtt.connect()

    discovery_published = False
    last_published: dict[str, Any] = {}
    last_publish_time = 0.0

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
                            discovery_published, last_publish_time = _process_frame(
                                frame,
                                settings,
                                mqtt,
                                last_published,
                                discovery_published,
                                last_publish_time,
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
                            discovery_published, last_publish_time = _process_frame(
                                frame,
                                settings,
                                mqtt,
                                last_published,
                                discovery_published,
                                last_publish_time,
                            )

        except Exception as exc:
            LOGGER.exception("Bridge loop failed: %s", exc)
            LOGGER.info("Retrying in %.1f seconds", backoff_seconds)
            time.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2, max_backoff_seconds)


if __name__ == "__main__":
    main()
