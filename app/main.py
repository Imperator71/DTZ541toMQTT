from __future__ import annotations

import logging
import os
import socket
import time
from typing import Any

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

    while True:
        try:
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

                while True:
                    chunk = sock.recv(4096)
                    if not chunk:
                        raise ConnectionError("Meter stream closed the connection")

                    for frame in extractor.feed(chunk):
                        result = parse_frame(frame)
                        if not result.values:
                            continue

                        meter_id = result.values.get("server_id")
                        if settings.mqtt_discovery and not discovery_published:
                            publish_homeassistant_discovery(
                                mqtt=mqtt,
                                settings=settings,
                                meter_id=meter_id if isinstance(meter_id, str) else None,
                            )
                            discovery_published = True

                        now = time.time()
                        should_publish = (
                            now - last_publish_time >= settings.publish_interval_seconds
                        )

                        if should_publish:
                            for key, value in result.values.items():
                                if last_published.get(key) != value:
                                    mqtt.publish_state(key, value, retain=True)
                                    LOGGER.info("Published %s=%s", key, value)
                                    last_published[key] = value
                            last_publish_time = now

        except Exception as exc:
            LOGGER.exception("Bridge loop failed: %s", exc)
            time.sleep(5)


if __name__ == "__main__":
    main()
