# Esp32Broker

[日本語](README.ja.md) | [Beginner's guide](../../docs/guide-beginner.md)

Makes an ESP32 an MQTT broker: MQTT over TCP (3.1.1 / 5.0, port 1883) and MQTT-SN over UDP (1.2, port 1884), relayed in one topic space. The sketch itself subscribes to `sensors/#` and prints what arrives.

## What you need

- An ESP32 board (arduino-esp32 3.3 or later)

## Settings

| Constant | Meaning |
|---|---|
| `kWifiSsid` / `kWifiPass` | Wi-Fi |

MQTT-SN clients can publish to `sensors/temperature` as predefined topic id 1, without connecting (QoS −1).

## What you see

```
TinyMQ Esp32Broker
Broker at 192.168.1.20
client in:  mosquitto_sub_1234
sensors/temperature = 21.5
```

`mosquitto_pub -h 192.168.1.20 -t sensors/temperature -m 21.5` from a PC is printed by the sketch. `mosquitto_sub -h 192.168.1.20 -t '#' -v` shows what other clients and MQTT-SN clients ([`Ch32SnSensor`](../Ch32SnSensor/README.md)) send.

## Notes

- The TCP listener is `TinyMQLwipListener` on lwIP's raw API. To accept through the Arduino `WiFiServer`, use `TinyMQTcpListener` ([`PicoWBroker`](../PicoWBroker/README.md)).
- MQTT-SN has no encryption or authentication: use it on a closed network or inside a TinyWG tunnel.
- Connections and memory are set with `-D` (see the [advanced guide](../../docs/guide-advanced.md)).
