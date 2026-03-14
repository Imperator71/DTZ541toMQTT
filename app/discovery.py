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
    "voltage_l1_v": {
        "name": "DTZ541 Spannung L1",
        "unit": "V",
        "device_class": "voltage",
        "state_class": "measurement",
        "icon": "mdi:sine-wave",
    },
    "voltage_l2_v": {
        "name": "DTZ541 Spannung L2",
        "unit": "V",
        "device_class": "voltage",
        "state_class": "measurement",
        "icon": "mdi:sine-wave",
    },
    "voltage_l3_v": {
        "name": "DTZ541 Spannung L3",
        "unit": "V",
        "device_class": "voltage",
        "state_class": "measurement",
        "icon": "mdi:sine-wave",
    },
    "current_l1_a": {
        "name": "DTZ541 Strom L1",
        "unit": "A",
        "device_class": "current",
        "state_class": "measurement",
        "icon": "mdi:current-ac",
    },
    "current_l2_a": {
        "name": "DTZ541 Strom L2",
        "unit": "A",
        "device_class": "current",
        "state_class": "measurement",
        "icon": "mdi:current-ac",
    },
    "current_l3_a": {
        "name": "DTZ541 Strom L3",
        "unit": "A",
        "device_class": "current",
        "state_class": "measurement",
        "icon": "mdi:current-ac",
    },
    "frequency_hz": {
        "name": "DTZ541 Frequenz",
        "unit": "Hz",
        "device_class": "frequency",
        "state_class": "measurement",
        "icon": "mdi:sine-wave",
    },
    "phase_angle_l2_l1_deg": {
        "name": "DTZ541 Winkel U-L2/U-L1",
        "unit": "deg",
        "state_class": "measurement",
        "icon": "mdi:angle-acute",
    },
    "phase_angle_l3_l1_deg": {
        "name": "DTZ541 Winkel U-L3/U-L1",
        "unit": "deg",
        "state_class": "measurement",
        "icon": "mdi:angle-acute",
    },
    "phase_angle_p1_deg": {
        "name": "DTZ541 Winkel I-L1/U-L1",
        "unit": "deg",
        "state_class": "measurement",
        "icon": "mdi:angle-acute",
    },
    "phase_angle_p2_deg": {
        "name": "DTZ541 Winkel I-L2/U-L2",
        "unit": "deg",
        "state_class": "measurement",
        "icon": "mdi:angle-acute",
    },
    "phase_angle_p3_deg": {
        "name": "DTZ541 Winkel I-L3/U-L3",
        "unit": "deg",
        "state_class": "measurement",
        "icon": "mdi:angle-acute",
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
