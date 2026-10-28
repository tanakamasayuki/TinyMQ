# Esp32Client

[日本語](README.ja.md) | [Beginner's guide](../../docs/guide-beginner.md)

Connects to an MQTT broker and publishes a counter to `tinymq/counter` every 5 s. It subscribes to `tinymq/cmd` and prints what arrives. On connecting it publishes `online` (retained) to `tinymq/status`; if it vanishes, its will publishes `offline` there.

## What you need

- An ESP32 board (arduino-esp32 3.3 or later)
- An MQTT broker (mosquitto on a PC, [`Esp32Broker`](../Esp32Broker/README.md) on another board, ...)

## Settings

| Constant | Meaning |
|---|---|
| `kWifiSsid` / `kWifiPass` | Wi-Fi |
| `kBroker` / `kBrokerPort` | Broker address and port |

## What you see

```
TinyMQ Esp32Client
Local IP: 192.168.1.50
connected
received tinymq/cmd: hello
```

On a PC, `mosquitto_sub -h <broker> -t 'tinymq/#' -v` shows the counter, and `mosquitto_pub -h <broker> -t tinymq/cmd -m hello` reaches the board.

## Notes

- It reconnects every 5 s when the connection drops (`setReconnectInterval()`).
- For MQTT 5.0, call `mqtt.setProtocolVersion(tinymq::kMqtt5)` before `connect()`.
