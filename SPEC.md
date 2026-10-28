# TinyMQ Specification (draft)

[日本語](SPEC.ja.md)

> Status: Phases 0 to 4 implemented (2026-10-28): MQTT 3.1.1 / 3.1 client and broker, MQTT-SN 1.2 gateway and client. Tested on the host only, not yet on boards. Values not yet decided are marked `TBD`.

## 1. Purpose and scope

**TinyMQ** is a portable MQTT library for Arduino. It contains **both a client and a broker**; a sketch uses only the side it needs, and the other side is not linked.

- The base is ordinary MQTT over TCP (3.1.1 / 5.0).
- The broker also accepts **MQTT-SN 1.2** over UDP and relays between those clients and the TCP clients in one topic space. Chips too small for TCP/MQTT join over UDP.
- The UDP-side client (MQTT-SN) is part of TinyMQ as well.
- The broker targets ESP32 / Pico W class hardware and **implements as much of the specifications as it can**; build-time and run-time settings strip features to make it lighter.

### 1.1 Use cases

| # | Setup | Role of TinyMQ |
|---|---|---|
| UC1 | ESP32 etc. → an MQTT broker in the cloud or on the LAN (mosquitto etc.) | `TinyMQClient` (TCP) |
| UC2 | An ESP32 / Pico W is the LAN broker; PCs, phones and other microcontrollers join over TCP, small MCUs (CH32V etc.) over UDP (MQTT-SN) | `TinyMQBroker` + TCP / MQTT-SN listeners; devices: `TinyMQClient` or `TinyMQClientSn` |
| UC3 | The UC2 broker shares the board with a TinyWG server; devices join through the tunnel (WireGuard provides the encryption and authentication the UDP side lacks) | as above + TinyWG |
| UC4 | The UC2 broker forwards some topics to an upstream (cloud) broker | `TinyMQBroker` + bridge |
| UC5 | The sketch itself publishes and subscribes inside the broker, without the network | the broker's local client |

### 1.2 Target platforms

The same cores as TinyWG.

| Core | Client | Broker | Notes |
|---|---|---|---|
| ESP32 / ESP32-S3 (arduino-esp32 3.3) | yes | yes (main target) | |
| RP2040 / RP2350 (arduino-pico 6.3, Pico W / Pico 2 W) | yes | yes (main target) | |
| CH32V (ArduinoCore-CH32RV 0.4, Network library) | yes (mainly MQTT-SN) | a reduced configuration `TBD` | |
| host (lang-ship:host) | yes | yes | `WiFiClient` / `WiFiServer` / `WiFiUDP` run on real sockets, so interop tests run against mosquitto and paho-mqtt |

The clients run on any core with a `Client&` or a `UDP&`, including Ethernet without lwIP (W5500 and the like).

## 2. Out of scope

- MQTT over WebSocket
- TLS on the broker side (on the client side, pass a `WiFiClientSecure` or similar as the `Client&`)
- MQTT-SN 2.0 (still in progress at OASIS; considered once stable)
- Clustering

## 3. Architecture

```
App (sketch)
 ├ TinyMQClient        MQTT 3.1.1 / 5.0 client (over an Arduino Client&)
 ├ TinyMQClientSn      MQTT-SN 1.2 client (over an Arduino UDP&)
 └ TinyMQBroker        the broker + a local client + bridges
        │ several listeners can be registered
        ├ TinyMQTcpListener<ServerT>   Arduino WiFiServer / NetworkServer / EthernetServer etc.
        ├ TinyMQSnListener             Arduino UDP& (MQTT-SN gateway)
        └ TinyMQLwipListener           lwIP tcp_pcb directly (later phase)
[tinymq::core]   codecs (3.1.1 / 5.0 / MQTT-SN), sessions, subscription table, topic matching,
                 retained messages, message pool      ← no Arduino or lwIP dependency
[Port]           time (millis-like), locking (lwIP listener only)
```

### 3.1 Layers

- **core**: plain C++. Input is a byte stream per connection (TCP) or a datagram (UDP); so is the output. Unit-tested on the host.
  - The internal representation follows **MQTT 5.0** (reason codes, properties, session expiry, ...). 3.1.1 is handled as a subset of 5.0, dropped at encoding time. MQTT-SN sessions live in the same session table.
- **Broker and listeners**: the core sees a connection only through `how much can be sent / send / close`. Several listeners can be registered at once; TCP and MQTT-SN clients are relayed in the same topic space without distinction.
- **External transports first**: as in TinyWG, the clients take a `Client&` / `UDP&`, and the broker's listeners take the user's `Server` / `UDP`. A listener on raw lwIP is added later as an optimisation.

### 3.2 Execution model

- The Arduino-API listeners and the clients **poll**: call `update()` from `loop()`. `update()` does not block (what cannot be written now is written next time).
- The `Server` base class has no `accept()` and its type differs between cores, so the TCP listener is a template that takes the client type from the return type of `accept()` / `available()`.
- The lwIP listener (later phase) runs from callbacks, with the same locking as TinyWG §3.2.
- The core assumes no threads. Locking for calls from several tasks is `TBD` (expected: a broker in its own task on ESP32).

## 4. Functional requirements

### 4.1 MQTT (TCP) — broker

The goal is all of MQTT 3.1.1 / 5.0. `[lite]` marks what settings can remove (§5).

| Feature | 3.1.1 | 5.0 | Notes |
|---|---|---|---|
| QoS 0 / 1 / 2 | yes | yes | QoS 2 delivery is `[lite]` (§4.3) |
| Retained messages | yes | yes | limits on count and total bytes. `[lite]` |
| Will | yes | yes | including 5.0 Will Delay Interval |
| Keep alive (disconnect at 1.5×) | yes | yes | 5.0 Server Keep Alive can override |
| Clean session / clean start + session expiry | yes | yes | |
| Persistent sessions (messages kept while offline) | yes | yes | per-client count and total byte limits. `[lite]` |
| Session takeover (same Client ID) | yes | yes | |
| Wildcards `+` / `#`, topics starting with `$` | yes | yes | |
| Authentication (user name / password) | yes | yes | decided by a callback |
| Authorisation (per-topic publish / subscribe) | yes | yes | decided by a callback |
| Assigned Client ID | yes | yes | |
| Inflight and receive limits | yes | yes | 5.0 Receive Maximum |
| Properties passed through (user properties, Content Type, Response Topic, Correlation Data, ...) | — | yes | |
| Topic aliases | — | yes | `[lite]` |
| Shared subscriptions `$share/` | — | yes | `[lite]` |
| Subscription identifiers, No Local, Retain As Published, Retain Handling | — | yes | |
| Message expiry | — | yes | |
| Maximum Packet Size, Maximum QoS, Retain Available, ... in CONNACK | — | yes | features removed by settings are announced correctly in CONNACK |
| Server DISCONNECT with reason code | — | yes | |
| Enhanced authentication (AUTH) | — | `TBD` | maybe only the callback shape |
| `$SYS` topics | yes | yes | `[lite]`. Contents follow mosquitto `TBD` |
| MQTT 3.1 (protocol name `MQIsdp`) | yes | — | for old clients; cheap to accept `TBD` |

### 4.2 MQTT (TCP) — client

- MQTT 3.1.1 / 5.0, QoS 0 / 1 / 2, retain, will, persistent sessions (resend on reconnect), keep alive, optional automatic reconnect
- 5.0: properties, topic aliases (both directions), reason codes
- Incoming messages through a callback (topic, payload, QoS, retain, 5.0 properties)
- Takes a `Client&`, so TLS works with a `WiFiClientSecure` or similar
- The broker's bridge uses this client

### 4.3 Removing QoS 2

QoS 2 costs differently in each direction.

- **Inbound (client → broker) is cheap**: the broker remembers only the packet identifier and forwards on receipt (Method B in 3.1.1 §4.3.3). No payload is kept; each client has a set of identifiers. **Always accepted.**
- **Delivery (broker → subscriber) is expensive**: the message is kept until PUBREC, then the PUBREL state is kept.

"Removing QoS 2" means capping delivery at QoS 1. In 3.1.1 the SUBACK grants a lower QoS (delivery is min(publish QoS, granted QoS)); 5.0 also announces Maximum QoS in CONNACK. Selectable both at build time (the code is removed) and at run time.

### 4.4 MQTT-SN 1.2 — broker side (gateway)

An aggregating gateway built into the broker. MQTT-SN clients enter the broker's session table directly (it does not translate to TCP MQTT for another broker).

| Feature | Notes |
|---|---|
| CONNECT / WILLTOPICREQ / WILLTOPIC / WILLMSGREQ / WILLMSG / CONNACK | |
| REGISTER / REGACK (from the client and from the gateway) | topic IDs are assigned per client |
| Predefined topic IDs | registered on the broker through the API; shared by all clients |
| Short topic names (2 characters) | |
| PUBLISH QoS −1 / 0 / 1 / 2 | QoS −1 accepted without a connection (predefined IDs and short names only). Can be disabled `[lite]` |
| SUBSCRIBE / UNSUBSCRIBE (topic name, predefined ID, short name, wildcards) | for wildcard subscriptions the gateway REGISTERs before PUBLISH |
| PINGREQ / PINGRESP, keep alive | |
| Sleeping clients (DISCONNECT with duration, asleep / awake, delivery on wake-up) | limit on buffered messages. `[lite]` |
| WILLTOPICUPD / WILLMSGUPD | |
| ADVERTISE / SEARCHGW / GWINFO | broadcast support depends on the core. `[lite]` |
| Retransmission (Tretry / Nretry) | |
| Forwarder Encapsulation | `TBD` |

Sessions are identified by the client's address and port (as in MQTT-SN 1.2). If the source changes after sleeping, the session is found by Client ID.

**Security**: MQTT-SN 1.2 has neither encryption nor authentication. It is meant for closed networks or inside a TinyWG tunnel, and the documentation says so. Each listener can have an allow-list of source addresses.

### 4.5 MQTT-SN 1.2 — client (`TinyMQClientSn`)

For small chips, so **features are chosen in steps at build time** (`TINYMQ_SN_CLIENT_LEVEL`).

| Level | Contents |
|---|---|
| 0 | QoS −1 PUBLISH only (predefined IDs and short names). No connection state |
| 1 | CONNECT, REGISTER, PUBLISH QoS 0 / 1, SUBSCRIBE, PINGREQ, retransmission |
| 2 (default) | the above + QoS 2, will, sleep, gateway discovery |

The default is 2 because the Arduino IDE makes `-D` hard to pass, and the linker drops the functions a sketch never calls at any level; a lower level shrinks what `update()` pulls in and the buffers.

- Procedures (connect, register, subscribe, QoS 1 / 2 publish) wait for the answer (blocking). Without one they resend every Tretry (PUBLISH with DUP) and give up after Nretry. Messages in flight are not copied and kept, so the RAM is the buffers and the topic table. Messages arriving meanwhile go to the callback.
- A hook called while waiting (`onIdle()`) keeps other work (such as a broker on the same board) running.
- The topic id table survives reconnecting: a gateway resuming a session after sleep keeps its registrations. A stale entry is overwritten by the gateway's REGISTER, or registered again after a PUBACK "invalid topic id".

CH32V307 (difference to a sketch sending UDP over Ethernet once a second, `TINYMQ_SN_CLIENT_BUFFER` 256, 2026-10-28):

| Level | Flash | RAM (static) |
|---|---|---|
| 0 | +1.0 KB | +0.4 KB |
| 1 | +5.1 KB | +1.2 KB |
| 2 | +6.0 KB | +1.2 KB |

### 4.6 Local client and bridge

- **Local client**: the sketch calls `publish()` / `subscribe()` inside the broker. No network; delivered under the same subscription, retain and QoS rules as the other clients.
- **Bridge**: the broker connects to an upstream broker with `TinyMQClient` and forwards according to a topic map (direction in / out / both, prefix rewriting, QoS), like mosquitto's bridge settings. Includes loop prevention (No Local on 5.0, prefixes on 3.1.1).
- Several upstreams `TBD`.

### 4.7 Persistence

- Hooks (save / load callbacks) to store retained messages and persistent sessions in flash or elsewhere. Concrete storage (LittleFS etc.) is shown in an example.
- Memory only by default (lost on reboot).

## 5. Non-functional requirements

| Item | Policy |
|---|---|
| Memory allocation | No allocation while running. Connection, session and subscription tables are static. Variable-length messages (retained, queued, inflight) live in a **message pool** (a byte area shared with reference counts, so delivering to several subscribers does not copy the payload). The pool is either a static array or allocated once in `begin()` (including ESP32 PSRAM). |
| Execution model | Polling (`update()`). The lwIP listener uses callbacks (§3.2) |
| Flash / RAM | Measured and recorded. Targets set after measuring |
| Throughput and latency | Measured and recorded (one-to-many delivery, per QoS) |
| Compatibility | Checked against mosquitto, paho-mqtt (Python) and the Eclipse interoperability test suite |

### 5.1 Settings

Two levels: build-time macros (override with `-D`, in `src/tinymq/mq_config.h`) and run-time setters (before or after `begin()`). A feature removed at build time cannot be enabled at run time. Defaults are decided after measuring.

| Macro (draft) | Meaning |
|---|---|
| `TINYMQ_BROKER_MAX_CLIENTS` | concurrent connections (TCP + MQTT-SN) |
| `TINYMQ_BROKER_MAX_SESSIONS` | sessions kept (including offline persistent sessions) |
| `TINYMQ_BROKER_MAX_SUBSCRIPTIONS` | total subscriptions |
| `TINYMQ_MAX_TOPIC_LEN` | longest topic name |
| `TINYMQ_MAX_PACKET` | longest packet accepted |
| `TINYMQ_BROKER_POOL_BYTES` | size of the message pool (0: allocated in `begin()`) |
| `TINYMQ_BROKER_MAX_RETAINED` | retained messages kept |
| `TINYMQ_BROKER_MAX_INFLIGHT` | inflight limit per client |
| `TINYMQ_BROKER_MAX_QUEUED` | queue per client while offline or asleep |
| `TINYMQ_ENABLE_MQTT5` | include 5.0 |
| `TINYMQ_ENABLE_QOS2_DELIVERY` | include QoS 2 delivery (§4.3) |
| `TINYMQ_ENABLE_PERSISTENT_SESSION` | persistent sessions |
| `TINYMQ_ENABLE_SHARED_SUB` / `TINYMQ_ENABLE_TOPIC_ALIAS` / `TINYMQ_ENABLE_SYS` | individual 5.0 features, `$SYS` |
| `TINYMQ_ENABLE_SN` | MQTT-SN gateway |
| `TINYMQ_SN_CLIENT_LEVEL` | MQTT-SN client level (§4.5), default 2 |
| `TINYMQ_ENABLE_BRIDGE` | bridge |

## 6. Testing

The same structure as TinyWG (pytest-embedded + Arduino CLI, the `TEST done N/M` protocol, cores pinned by `sketch.yaml` profiles).

- **Host unit tests**: codec encode / decode (examples from the specifications and malformed packets), topic matching, subscription table, message pool
- **Host interop** (real sockets on lang-ship:host)
  - `TinyMQClient` ↔ mosquitto
  - paho-mqtt (Python) ↔ `TinyMQBroker`: delivery per QoS, retain, will, persistent sessions, 5.0 features
  - The Eclipse paho.mqtt.testing interoperability suite against `TinyMQBroker` (used only as a test tool, not distributed)
  - MQTT-SN: a Python MQTT-SN peer written for the tests (like TinyWG's `wg_peer.py`) ↔ `TinyMQBroker` / `TinyMQClientSn`; other MQTT-SN implementations where possible
  - Mixed TCP and MQTT-SN (publish over MQTT-SN, receive with paho, and the reverse)
- **On board**: ESP32 / ESP32-S3 / Pico W as the broker, CH32V307 as an MQTT-SN client; the setup through a TinyWG tunnel (UC3)
- **Measurements**: footprint, RAM per number of connections, delivery throughput and latency

## 7. Plan

In the order that is easiest to build; everything is implemented in the end.

1. **Phase 0 — skeleton**: `library.properties`, the `src/` layout, `tests/` (pytest, `sketch.yaml`), `tools/bump_version.py` and the release workflow, taken from TinyWG
2. **Phase 1 — core**: 3.1.1 codec (internal representation on 5.0), topic matching, subscription table, message pool; host unit tests
3. **Phase 2 — `TinyMQClient` (3.1.1)**: QoS 0 / 1 / 2, retain, will, persistent sessions; interop with mosquitto
4. **Phase 3 — `TinyMQBroker` (3.1.1) + `TinyMQTcpListener`**: all of 3.1.1, the local client; paho-mqtt and the interop suite
5. **Phase 4 — MQTT-SN**: codec, `TinyMQSnListener`, `TinyMQClientSn`; the Python MQTT-SN peer, mixed with TCP
6. **Phase 5 — on board**: ESP32 / ESP32-S3 / Pico W / CH32V307, measurements, together with TinyWG
7. **Phase 6 — MQTT 5.0**: codec properties, client, broker 5.0 features
8. **Phase 7 — bridge**
9. **Phase 8 — lwIP listener and persistence hooks**
10. Defaults and presets (lite / standard / full) from the measurements

## 8. License and trademarks

- TinyMQ is **MIT**. No third-party code is bundled.
- The protocol layers are implemented from the public specifications (OASIS MQTT 3.1.1, MQTT 5.0, MQTT-SN 1.2). No code from existing implementations (Eclipse Paho, mosquitto, ...) is used.
- mosquitto, paho-mqtt, paho.mqtt.testing and the like are **used only as test peers** and are not distributed.
- "MQTT" is used descriptively as the protocol name.
