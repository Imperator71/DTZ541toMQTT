# DTZ541toMQTT

Small Holley DTZ541-ZEBA to MQTT bridge for Home Assistant.

This project is intended for setups like:

```text
Holley DTZ541-ZEBA -> Wattwächter (USB) -> TrueNAS -> ser2net -> DTZ541toMQTT -> MQTT -> Home Assistant

 
It can now run in two input modes:

- Direct serial: pass the USB reader device into the container and set `SERIAL_DEVICE`
- TCP stream: keep using `ser2net` and set `TCP_HOST` and `TCP_PORT`

Direct serial mode is preferred when your container runtime can pass through the reader device.

Environment variables:

- `SERIAL_DEVICE`: serial device path inside the container, for example `/dev/ttyUSB0`
- `SERIAL_BAUD`: serial baud rate, default `9600`
- `SERIAL_TIMEOUT_SECONDS`: serial read timeout, default `2`
- `TCP_HOST`: required only when `SERIAL_DEVICE` is not set
- `TCP_PORT`: TCP port for `ser2net`, default `2001`
- `MQTT_HOST`: MQTT broker host
- `MQTT_PORT`: MQTT broker port, default `1883`
- `MQTT_USERNAME`: optional MQTT username
- `MQTT_PASSWORD`: optional MQTT password
- `MQTT_TOPIC_PREFIX`: topic prefix, default `dtz541`
- `MQTT_CLIENT_ID`: MQTT client id, default `dtz541tomqtt`
- `MQTT_DISCOVERY`: Home Assistant discovery toggle, default `true`
- `MQTT_DISCOVERY_PREFIX`: discovery prefix, default `homeassistant`
- `LOG_LEVEL`: log level, default `INFO`
- `LOG_RAW_FRAMES`: log raw hex frames for debugging, default `false`
- `LOG_PARSED_FRAMES`: log parsed values for debugging, default `false`
- `PUBLISH_INTERVAL_SECONDS`: minimum publish interval, default `5`
- `SOCKET_TIMEOUT_SECONDS`: TCP socket timeout, default `15`

Example direct serial setup:

```text
Holley DTZ541-ZEBA -> Wattwächter (USB) -> TrueNAS -> DTZ541toMQTT -> MQTT -> Home Assistant
```
