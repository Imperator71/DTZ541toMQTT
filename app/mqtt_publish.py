from __future__ import annotations

import json
import logging
import threading
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

        self._connected = threading.Event()
        self._republish_requested = threading.Event()

        status_topic = f"{self.settings.mqtt_topic_prefix}/status"
        self.client.will_set(status_topic, payload="offline", qos=0, retain=True)

        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message

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

    @property
    def connected(self) -> bool:
        return self._connected.is_set()

    def wait_until_connected(self, timeout: float = 10.0) -> bool:
        return self._connected.wait(timeout)

    def consume_republish_request(self) -> bool:
        if not self._republish_requested.is_set():
            return False
        self._republish_requested.clear()
        return True

    def publish_value(
        self,
        topic: str,
        payload: Any,
        *,
        retain: bool = True,
        qos: int = 0,
    ) -> bool:
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
            return False
        return True

    def publish_availability(self, online: bool) -> bool:
        topic = f"{self.settings.mqtt_topic_prefix}/status"
        payload = "online" if online else "offline"
        return self.publish_value(topic, payload, retain=True)

    def publish_state(self, key: str, payload: Any, *, retain: bool = True) -> bool:
        topic = f"{self.settings.mqtt_topic_prefix}/{key}"
        return self.publish_value(topic, payload, retain=retain)

    def _on_connect(
        self,
        client: mqtt.Client,
        userdata: Any,
        flags: mqtt.ConnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None,
    ) -> None:
        LOGGER.info("Connected to MQTT broker with reason_code=%s", reason_code)
        if reason_code.is_failure:
            LOGGER.error("MQTT connection rejected: %s", reason_code)
            return

        self._connected.set()
        self.publish_availability(True)

        if self.settings.mqtt_discovery:
            birth_topic = f"{self.settings.mqtt_discovery_prefix}/status"
            result, _ = client.subscribe(birth_topic, qos=0)
            if result != mqtt.MQTT_ERR_SUCCESS:
                LOGGER.warning("MQTT subscribe failed rc=%s for topic=%s", result, birth_topic)

        # Discovery and retained states must be replayed after every MQTT (re)connect.
        self._republish_requested.set()

    def _on_disconnect(
        self,
        client: mqtt.Client,
        userdata: Any,
        flags: mqtt.DisconnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None,
    ) -> None:
        self._connected.clear()
        LOGGER.warning("Disconnected from MQTT broker with reason_code=%s", reason_code)

    def _on_message(
        self,
        client: mqtt.Client,
        userdata: Any,
        message: mqtt.MQTTMessage,
    ) -> None:
        birth_topic = f"{self.settings.mqtt_discovery_prefix}/status"
        if message.topic != birth_topic:
            return

        payload = message.payload.decode("utf-8", errors="replace").strip().lower()
        if payload == "online":
            LOGGER.info("Home Assistant birth message received; scheduling discovery replay")
            self._republish_requested.set()
