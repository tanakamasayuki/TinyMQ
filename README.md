# TinyMQ

[日本語](README.ja.md)

A portable MQTT library for Arduino. It includes **both a client and a broker**, and only the side you use is linked.

- `TinyMQClient`: an MQTT 3.1.1 / 5.0 client over any Arduino `Client`. Pass a `WiFiClientSecure` for TLS
- `TinyMQBroker`: an MQTT 3.1.1 / 3.1 / 5.0 broker. QoS 0 / 1 / 2, retain, will, persistent sessions, shared subscriptions, authentication and authorization callbacks, and publish / subscribe from the sketch itself
- `TinyMQSnListener`: an MQTT-SN 1.2 gateway built into the broker. Small chips for which TCP is too heavy join over UDP and share one topic space with the TCP clients
- `TinyMQClientSn`: an MQTT-SN 1.2 client over any Arduino `UDP`. About 1 KB of flash when it only sends at QoS −1
- `TinyMQBridge`: a bridge from the broker to an upstream broker (like mosquitto's bridge)
- The protocol code (the `tinymq::` namespace) depends on neither lwIP nor the Arduino API. It is tested automatically against mosquitto and paho-mqtt

Some uses:

- **As a plain MQTT client**: from an ESP32 or similar to a broker in the cloud or on the LAN
- **Make a board the broker**: an ESP32 / Pico W is the broker on the LAN; PCs, phones and other boards connect over TCP, and small MCUs (CH32V and others) over UDP with MQTT-SN
- **A gateway on site**: collect on the board's broker and bridge only the topics needed to an upstream broker
- **Together with TinyWG**: MQTT-SN has neither encryption nor authentication, so use it inside a [TinyWG](https://github.com/tanakamasayuki/TinyWG) tunnel

## Documentation

- [Beginner's guide](docs/guide-beginner.md): from making an ESP32 a client to making a board the broker and connecting small chips over MQTT-SN
- [Advanced guide](docs/guide-advanced.md): structure, memory and configuration, choosing a listener, QoS and sessions, MQTT 5.0, MQTT-SN, bridges, persistence, authentication and authorization, execution model
- [API reference](docs/api.md): every public class and method
- [Specification](SPEC.md): design and measurements
- [Tests](tests/README.md): the test suites and how to run them

## Examples

| Example | What it does |
|---|---|
| [`Esp32Client`](examples/Esp32Client/README.md) | Connects to a broker, publishes every 5 s and subscribes to commands |
| [`Esp32Broker`](examples/Esp32Broker/README.md) | Makes an ESP32 the broker, with TCP (lwIP), an MQTT-SN gateway and the sketch's own subscription |
| [`Esp32Bridge`](examples/Esp32Bridge/README.md) | Bridges from the local broker to an upstream broker, rewriting topics |
| [`Esp32Persist`](examples/Esp32Persist/README.md) | Saves retained messages and persistent sessions to LittleFS and keeps them across restarts |
| [`PicoWBroker`](examples/PicoWBroker/README.md) | Makes a Pico W a broker that accepts connections with Arduino's `WiFiServer` |
| [`Ch32SnSensor`](examples/Ch32SnSensor/README.md) | A CH32V with Ethernet sends values and receives commands over MQTT-SN |

## Status

| Core | Client | Broker | MQTT-SN | Verified |
|---|---|---|---|---|
| ESP32 / ESP32-S3 (arduino-esp32 3.3) | yes | yes (`TinyMQLwipListener` / `TinyMQTcpListener`) | gateway / client | on board (all interop tests over Wi-Fi) |
| RP2040 / RP2350 (arduino-pico 6.3, Pico W / Pico 2 W) | yes | yes (no `TinyMQLwipListener` in FreeRTOS mode) | gateway / client | build only |
| CH32V (ArduinoCore-CH32RV 0.4, Network library) | yes | yes (configured smaller) | mainly client | build only |
| lang-ship:host (tests on a PC) | yes | yes (`TinyMQTcpListener`) | gateway / client | interop-tested against mosquitto / paho-mqtt |

On any core with `Client` / `UDP`, the client should run as a rule (Ethernet libraries such as W5500 included).

### Footprint (added to a sketch with the same features)

| | Flash | RAM (static) |
|---|---|---|
| ESP32 `TinyMQClient` (default configuration) | +11.9 KB | +4.9 KB |
| ESP32 broker (TCP + MQTT-SN, default configuration) | +32 KB | +48 KB |
| CH32V307 `TinyMQClientSn` level 0 (QoS −1 send only) | +1.0 KB | +0.4 KB |
| CH32V307 `TinyMQClientSn` level 2 (everything) | +6.0 KB | +1.2 KB |

The broker's RAM is mostly the receive buffer per connection (default 8 × 1 KB), the send queue per session, and the message pool (default 16 KB). A broker cut down to 4 connections and a 4 KB pool on a CH32V307 fits in 59 KB flash / 47 KB RAM (including about 37 KB for lwIP). No dynamic allocation (one setting allocates the pool once in `begin()`).

## Usage (client)

```cpp
#include <WiFi.h>
#include <TinyMQ.h>

WiFiClient net;
TinyMQClient mqtt(net);

void onMessage(const tinymq::Message &m, void *)
{
    Serial.printf("%s: %.*s\n", m.topic, (int)m.payloadLen, (const char *)m.payload);
}

void setup()
{
    // ... join Wi-Fi
    mqtt.setServer(IPAddress(192, 168, 1, 10), 1883);
    mqtt.setClientId("esp32-1");
    mqtt.onMessage(onMessage);
    mqtt.connect();
    mqtt.subscribe("cmd/#", 1);
}

void loop()
{
    mqtt.update();
    // mqtt.publish("sensors/t", "21.5", 1);
}
```

## Usage (broker)

```cpp
#include <WiFi.h>
#include <WiFiUdp.h>
#include <TinyMQ.h>

TinyMQBroker broker;
TinyMQLwipListener tcp(1883);   // MQTT 3.1.1 / 5.0
WiFiUDP udp;
TinyMQSnListener sn(udp);       // MQTT-SN 1.2

void setup()
{
    // ... join Wi-Fi
    broker.begin();
    tcp.begin();
    udp.begin(1884);
    broker.addListener(tcp);
    broker.addListener(sn);
}

void loop()
{
    broker.update();
}
```

With `broker.subscribe()` / `broker.publish()`, the sketch itself can subscribe and publish inside the broker.

## Usage (MQTT-SN client)

```cpp
NetworkUDP udp;                 // WiFiUDP on an ESP32
TinyMQClientSn sn(udp);

void setup()
{
    // ... connect to the network
    udp.begin(0);
    sn.setGateway(IPAddress(192, 168, 1, 20), 1884);
    sn.publishPredefined(1, "21.5");      // QoS -1: can be sent without connecting
    sn.connect("ch32-1");                 // once connected, subscriptions and QoS 1 / 2 are available too
    sn.subscribe("cmd/#", 1);
}

void loop()
{
    sn.update();
}
```

## Build-time configuration

Override with `-D` (`src/tinymq/mq_config.h`). The main ones:

| Macro | Default | |
|---|---|---|
| `TINYMQ_MAX_PACKET` | 1024 | largest packet accepted (the client's buffer) |
| `TINYMQ_BROKER_MAX_CLIENTS` | 8 | concurrent connections to the broker (TCP and MQTT-SN combined) |
| `TINYMQ_BROKER_MAX_SESSIONS` | 16 | sessions kept (including disconnected persistent sessions) |
| `TINYMQ_BROKER_POOL_BYTES` | 16384 | message pool; 0 allocates it in `begin()` |
| `TINYMQ_ENABLE_MQTT5` | 1 | the broker accepts 5.0 |
| `TINYMQ_ENABLE_QOS2_DELIVERY` | 1 | QoS 2 delivery from the broker to subscribers |
| `TINYMQ_SN_CLIENT_LEVEL` | 2 | feature level of the MQTT-SN client (0 / 1 / 2) |

Every setting is in the [advanced guide](docs/guide-advanced.md).

## Limitations

- IPv4 only (MQTT-SN gateway / client)
- TLS on the broker side, MQTT over WebSocket and MQTT-SN 2.0 are out of scope
- While upstream is down, the bridge keeps QoS 1 / 2 messages up to `TINYMQ_BRIDGE_QUEUE_BYTES` (2 KB by default); a restart loses them
- In arduino-pico's FreeRTOS mode, `TinyMQLwipListener` cannot be used (use `TinyMQTcpListener`)

## License

MIT. No third-party code is included. The protocols are implemented from the public OASIS specifications (MQTT 3.1.1, MQTT 5.0, MQTT-SN 1.2).
