# Ch32SnSensor

[日本語](README.ja.md) | [Beginner's guide](../../docs/guide-beginner.md)

Makes a CH32V with Ethernet an MQTT-SN client. Every 10 s it publishes its uptime in seconds to `sensors/uptime`, and `cmd/led` switches an LED (`on` or anything else). When it is not connected to the gateway, it sends to predefined topic id 1 with QoS −1 (no connection).

## What you need

- A CH32V with Ethernet (CH32V307 and others; ArduinoCore-CH32RV 0.4 or later, Network library)
- An MQTT-SN gateway ([`Esp32Broker`](../Esp32Broker/README.md), ...)

## Settings

| Constant | Meaning |
|---|---|
| `kGateway` / `kGatewayPort` | Gateway address and port |

## What you see

```
TinyMQ Ch32SnSensor
Local IP: 192.168.1.40
connected
led on
```

`mosquitto_sub -h <broker> -t 'sensors/#' -v` on the broker side shows the readings, and `mosquitto_pub -h <broker> -t cmd/led -m on` lights the LED.

## Notes

- `connect()`, `subscribe()` and QoS 1 `publish()` wait for the gateway's answer (blocking).
- `-DTINYMQ_SN_CLIENT_LEVEL=0` leaves only QoS −1 publishing and makes it smaller still (see the [advanced guide](../../docs/guide-advanced.md)).
- Not yet run on a board (build only).
