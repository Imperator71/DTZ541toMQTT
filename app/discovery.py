from __future__ import annotations

import logging
from typing import Any

from app.config import Settings
from app.mqtt_publish import MqttPublisher

LOGGER = logging.getLogger(__name__)


SENSOR_DEFINITIONS: dict[str, dict[str, Any]] = {
    "energy_import_kwh": {
        "name": "DTZ541 Bezug",
        "unit": "kWh",
        "device_class": "energy",
        "state_class": "total_increasing",
        "icon": "mdi:transmission-tower-import",
    },
    "energy_export_kwh": {
        "name": "DTZ541 Einspeisung",
        "unit": "kWh",
        "device_class": "energy",
        "state_class": "total_increasing",
        "icon": "mdi:transmission-tower-export",
    },
    "power_w": {
        "name": "DTZ541 Aktuelle Leistung",
        "unit": "W",
        "device_class": "power",
        "state_class": "measurement",
        "icon": "mdi:flash",
    },
    "server_id": {
        "name": "DTZ541 Zählernummer",
        "icon": "mdi:identifier",
    },
}


def publish_homeassistant_discovery(
    mqtt: MqttPublisher,
    settings: Settings,
    meter_id: str | None,
) -> None:
    """
    Publish MQTT discovery config for the currently supported sensors.
    """
    if not settings.mqtt_discovery:
        LOGGER.info("MQTT discovery disabled")
        return

    sanitized_meter_id = _sanitize_id(meter_id) if meter_id else "unknown-meter"
    device_identifier = f"{settings.mqtt_topic_prefix}_{sanitized_meter_id}"

    device_payload = {
        "identifiers": [device_identifier],
        "name": f"DTZ541 {sanitized_meter_id}",
        "manufacturer": "Holley / SmartCircuits",
        "model": "DTZ541-ZEBA via Wattwächter",
    }

    availability = [
        {
            "topic": f"{settings.mqtt_topic_prefix}/status",
            "payload_available": "online",
            "payload_not_available": "offline",
        }
    ]

    for sensor_key, meta in SENSOR_DEFINITIONS.items():
        object_id = f"{device_identifier}_{sensor_key}"
        topic = (
            f"{settings.mqtt_discovery_prefix}/sensor/"
            f"{device_identifier}/{sensor_key}/config"
        )

        payload: dict[str, Any] = {
            "name": meta["name"],
            "unique_id": object_id,
            "state_topic": f"{settings.mqtt_topic_prefix}/{sensor_key}",
            "availability": availability,
            "device": device_payload,
            "object_id": object_id,
        }

        if "unit" in meta:
            payload["unit_of_measurement"] = meta["unit"]
        if "device_class" in meta:
            payload["device_class"] = meta["device_class"]
        if "state_class" in meta:
            payload["state_class"] = meta["state_class"]
        if "icon" in meta:
            payload["icon"] = meta["icon"]

        mqtt.publish_value(topic, payload, retain=True)
        LOGGER.info("Published MQTT discovery for %s", sensor_key)


def _sanitize_id(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in value.strip())
    cleaned = cleaned.strip("_")
    return cleaned or "unknown-meter"
