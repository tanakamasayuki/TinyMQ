# PicoWBroker

[日本語](README.ja.md) | [Beginner's guide](../../docs/guide-beginner.md)

Makes a Pico W / Pico 2 W an MQTT broker, accepting through the Arduino `WiFiServer` (`TinyMQTcpListener`). The sketch subscribes to `#` and prints everything that arrives.

## What you need

- Pico W / Pico 2 W (arduino-pico 6.3 or later, not in FreeRTOS mode)

## Settings

| Constant | Meaning |
|---|---|
| `kWifiSsid` / `kWifiPass` | Wi-Fi |

## What you see

```
TinyMQ PicoWBroker
Broker at 192.168.1.30
test/x = hello
```

`mosquitto_pub -h 192.168.1.30 -t test/x -m hello` is printed.

## Notes

- `TinyMQTcpListener` works with any Arduino server that has `accept()` or `available()` (`WiFiServer`, `NetworkServer`, `EthernetServer`, ...). To accept on lwIP's raw API, use `TinyMQLwipListener`.
- Not yet run on a board (build only).
