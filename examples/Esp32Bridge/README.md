# Esp32Bridge

[日本語](README.ja.md) | [Advanced guide](../../docs/guide-advanced.md)

Bridges the ESP32 broker to an upstream broker: local `sensors/#` goes upstream as `site1/sensors/#`, and upstream `site1/cmd/#` reaches local clients as `cmd/#`.

## What you need

- An ESP32 board (arduino-esp32 3.3 or later)
- An upstream MQTT broker (mosquitto in the cloud or on the LAN, ...)

## Settings

| Constant | Meaning |
|---|---|
| `kWifiSsid` / `kWifiPass` | Wi-Fi |
| `kUpstreamHost` / `kUpstreamPort` | Upstream broker |

## What you see

Every 10 s, the upstream link and the forwarding counts:

```
TinyMQ Esp32Bridge
Broker at 192.168.1.20
upstream up, out 3, in 1, dropped 0
```

`mosquitto_pub -h 192.168.1.20 -t sensors/t -m 1` locally arrives upstream on `site1/sensors/t`.

## Notes

- On a 5.0 upstream connection the bridge subscribes with No Local, so `kBoth` topics do not loop. On 3.1.1 give the two sides different prefixes.
- Messages going out while upstream is down are dropped and counted (`dropped`). Every connect subscribes upstream again and takes the local retained messages up again.
- For TLS, make the `WiFiClient` a `WiFiClientSecure` and set its certificates.
