from __future__ import annotations

import json
import logging
from typing import Any

import paho.mqtt.client as mqtt

from app.config import Settings

LOGGER = logging.getLogger(__name__)


class MqttPublisher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=settings.mqtt_client_id,
            clean_session=True,
        )

        if settings.mqtt_username:
            self.client.username_pw_set(
                username=settings.mqtt_username,
                password=settings.mqtt_password or None,
            )

        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect

    def connect(self) -> None:
        LOGGER.info(
            "Connecting to MQTT broker at %s:%s",
            self.settings.mqtt_host,
            self.settings.mqtt_port,
        )
        self.client.connect(
            host=self.settings.mqtt_host,
            port=self.settings.mqtt_port,
            keepalive=60,
        )
        self.client.loop_start()

    def disconnect(self) -> None:
        try:
            self.client.loop_stop()
        finally:
            self.client.disconnect()

    def publish_value(
        self,
        topic: str,
        payload: Any,
        *,
        retain: bool = True,
        qos: int = 0,
    ) -> None:
        if isinstance(payload, (dict, list)):
            raw_payload = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
        else:
            raw_payload = str(payload)

        result = self.client.publish(topic, raw_payload, qos=qos, retain=retain)
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            LOGGER.warning(
                "MQTT publish returned non-success rc=%s for topic=%s",
                result.rc,
                topic,
            )

    def publish_availability(self, online: bool) -> None:
        topic = f"{self.settings.mqtt_topic_prefix}/status"
        payload = "online" if online else "offline"
        self.publish_value(topic, payload, retain=True)

    def publish_state(self, key: str, payload: Any, *, retain: bool = True) -> None:
        topic = f"{self.settings.mqtt_topic_prefix}/{key}"
        self.publish_value(topic, payload, retain=retain)

    def _on_connect(
        self,
        client: mqtt.Client,
        userdata: Any,
        flags: mqtt.ConnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None,
    ) -> None:
        LOGGER.info("Connected to MQTT broker with reason_code=%s", reason_code)
        self.publish_availability(True)

    def _on_disconnect(
        self,
        client: mqtt.Client,
        userdata: Any,
        flags: mqtt.DisconnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None,
    ) -> None:
        LOGGER.warning("Disconnected from MQTT broker with reason_code=%s", reason_code)
