# Advanced guide

[日本語](guide-advanced.ja.md) | [README](../README.md) | [Beginner's guide](guide-beginner.md) | [API reference](api.md)

How TinyMQ is built, how to configure it, which parts to use when, and what to watch for. The basics of using it are in the [beginner's guide](guide-beginner.md).

## 1. Structure

```
Sketch
 ├ TinyMQClient        MQTT 3.1.1 / 5.0 client (over Arduino Client)
 ├ TinyMQClientSn      MQTT-SN 1.2 client (over Arduino UDP)
 └ TinyMQBroker        broker + the sketch's own subscriptions + taps
        ├ TinyMQTcpListener<Server>   accepts TCP through an Arduino server
        ├ TinyMQLwipListener          accepts TCP through lwIP's raw API
        ├ TinyMQSnListener            MQTT-SN gateway (Arduino UDP)
        └ TinyMQBridge                bridge to an upstream broker (a TinyMQClient inside)
tinymq::               protocol handling (no dependency on Arduino or lwIP)
  ClientEngine / Broker / SnGateway / SnClientEngine
  codecs (3.1.1 / 5.0 / MQTT-SN), properties, topic matching, message pool
```

The Arduino classes are thin adapters between the `tinymq::` engines and Arduino's `Client` / `Server` / `UDP` or lwIP. `engine()` gives you the engine itself. To use another transport (a module of your own, a different TCP/IP stack), you can pass bytes to the engine directly (implement `tinymq::Link`, and hand received data over with `rxBuffer()` / `rxCommit()`).

Inside the broker, TCP clients, MQTT-SN clients, the sketch itself and bridges share one topic space and one session table. The gateway maps each MQTT-SN client onto one broker connection (§7).

## 2. Memory and configuration

### No dynamic allocation

All tables and buffers are allocated statically inside the classes. Variable-length data (retained messages, queued messages, wills, Client IDs, subscription filters) lives in the broker's **message pool**. When one message goes to several subscribers, its body is shared by reference count, not copied.

With `TINYMQ_BROKER_POOL_BYTES` set to 0, the pool is allocated once by `begin(bytes)` (for example, to use PSRAM on the ESP32).

### Settings (build time, override with `-D`)

| Macro | Default | Meaning |
|---|---|---|
| `TINYMQ_MAX_TOPIC_LEN` | 128 | Maximum length of a topic name |
| `TINYMQ_MAX_PACKET` | 1024 | Client receive / send buffer. Default for the broker's receive buffer |
| `TINYMQ_CLIENT_POOL_BYTES` | 2048 | Space where the client keeps QoS 1 / 2 messages in flight |
| `TINYMQ_CLIENT_MAX_INFLIGHT` | 8 | QoS 1 / 2 messages a client can have in flight at once |
| `TINYMQ_CLIENT_MAX_QOS2_IN` | 8 | QoS 2 messages a client is receiving (for duplicate detection) |
| `TINYMQ_BROKER_MAX_CLIENTS` | 8 | Concurrent broker connections (TCP and MQTT-SN combined) |
| `TINYMQ_BROKER_MAX_SESSIONS` | 16 | Sessions (including disconnected persistent sessions) |
| `TINYMQ_BROKER_MAX_SUBSCRIPTIONS` | 64 | Total subscriptions (including those of the sketch and bridges) |
| `TINYMQ_BROKER_MAX_RETAINED` | 32 | Number of retained messages |
| `TINYMQ_BROKER_MAX_QUEUED` | 32 | Queued messages per session (including those in flight) |
| `TINYMQ_BROKER_MAX_INFLIGHT` | 8 | QoS 1 / 2 messages in flight per session |
| `TINYMQ_BROKER_MAX_QOS2_IN` | 16 | QoS 2 messages being received per session. Advertised as Receive Maximum in 5.0 |
| `TINYMQ_BROKER_RX_BUFFER` | `TINYMQ_MAX_PACKET` | Receive buffer per connection = the largest packet a client can send |
| `TINYMQ_BROKER_POOL_BYTES` | 16384 | Message pool (0: allocated by `begin()`) |
| `TINYMQ_BROKER_MAX_LISTENERS` | 4 | Number of listeners |
| `TINYMQ_BROKER_MAX_TAPS` | 2 | Number of bridges |
| `TINYMQ_BROKER_TOPIC_ALIAS_MAX` | 8 | Topic aliases a 5.0 client may use (0: none) |
| `TINYMQ_ENABLE_MQTT5` | 1 | Accept 5.0 in the broker |
| `TINYMQ_ENABLE_SHARED_SUB` | 1 | Shared subscriptions |
| `TINYMQ_ENABLE_QOS2_DELIVERY` | 1 | QoS 2 delivery to subscribers |
| `TINYMQ_SN_MAX_CLIENTS` | 8 | MQTT-SN clients (each uses one broker connection) |
| `TINYMQ_SN_MAX_TOPICS` | 64 | Total topic ID registrations |
| `TINYMQ_SN_MAX_PREDEFINED` | 16 | Predefined topics |
| `TINYMQ_SN_POOL_BYTES` | 4096 | Space for registered topic names |
| `TINYMQ_SN_CLIENT_LEVEL` | 2 | MQTT-SN client level (§7) |
| `TINYMQ_SN_CLIENT_BUFFER` | 256 | Maximum datagram length for the MQTT-SN client |
| `TINYMQ_SN_CLIENT_MAX_TOPICS` | 8 | Topic IDs the MQTT-SN client remembers |
| `TINYMQ_SN_CLIENT_TOPIC_LEN` | 32 | Maximum length of those topic names |
| `TINYMQ_BRIDGE_MAX_TOPICS` | 8 | Topic mappings per bridge |

The Arduino IDE has no place to pass `-D`. With arduino-cli, use `--build-property "compiler.cpp.extra_flags=-DTINYMQ_BROKER_MAX_CLIENTS=4"`; with PlatformIO, use `build_flags`. The settings apply to the whole library, so a `#define` in the sketch does not change them.

### Estimates

The broker's RAM is roughly the sum of the following (on a 32-bit microcontroller).

- Connections: `MAX_CLIENTS × (RX_BUFFER + about 100)`
- Sessions: `MAX_SESSIONS × (about 40 + MAX_QUEUED × 16 + MAX_QOS2_IN × 2)`
- Subscriptions: `MAX_SUBSCRIPTIONS × about 24`; retained: `MAX_RETAINED × 4`
- Message pool: `POOL_BYTES`; send buffer: `MAX_PACKET`
- With the MQTT-SN gateway: about `3 × MAX_PACKET + SN_POOL_BYTES + SN_MAX_TOPICS × 12`

With the defaults, this is about 48 KB on an ESP32 (gateway included). On a CH32V307 with `MAX_CLIENTS=4 MAX_SESSIONS=6 POOL_BYTES=4096 MAX_PACKET=512 MAX_QUEUED=8`, it fits in 47 KB of RAM, lwIP included.

Code for features you do not use (5.0, MQTT-SN, bridges) is dropped by the linker as long as you do not call it.

## 3. Choosing a listener

| Listener | Suited to |
|---|---|
| `TinyMQLwipListener` | Cores with lwIP (ESP32, Pico W, CH32V). Light, with no sockets. It holds back delivery based on free space in the TCP send buffer, so a client that reads slowly does not stall the broker |
| `TinyMQTcpListener<Server>` | Networks without lwIP (the W5500 Ethernet library, for example) and host. It relies on Arduino's `Client::write()`, so a write can wait when the other side is not reading |
| `TinyMQSnListener` | MQTT-SN clients |
| `TinyMQBridge` | Forwarding to and from an upstream broker |

You can register several at once (for example, lwIP for Wi-Fi and `TinyMQTcpListener` for a W5500). The connection limit `TINYMQ_BROKER_MAX_CLIENTS` is the total over all of them.

## 4. Execution model

- Everything is **polled from `loop()`**. Keep calling `TinyMQClient::update()`, `TinyMQBroker::update()` and `TinyMQClientSn::update()`
- **Where it waits**: `TinyMQClient::connect()` (the TCP connection and CONNACK), the `TinyMQClientSn` procedures (until the response, retransmissions included), and writes in `TinyMQTcpListener` (depending on the Client). With `connectAsync()`, it does not wait for CONNACK. `TinyMQClientSn::onIdle()` lets other work run while it waits
- **Inside callbacks**: publishing from `onMessage` and the like is fine. From `TinyMQClientSn` callbacks, do not call the procedures that wait (`connect()`, QoS 1 / 2 publish, `subscribe()`)
- **Tasks**: use the broker and clients from one task only (usually `loop()`). If you call them from several tasks, serialise the calls yourself
- **lwIP**: `TinyMQLwipListener` callbacks only store received data; the broker runs inside `update()` while holding the lwIP lock (ESP32: the core lock, arduino-pico: `LWIPMutex`, CH32V: none needed). `TinyMQLwipListener` cannot be used in arduino-pico's FreeRTOS mode

## 5. QoS, sessions and queues

### Client

- QoS 1 / 2 messages sent are kept in the pool (`TINYMQ_CLIENT_POOL_BYTES`) until acknowledged, up to `TINYMQ_CLIENT_MAX_INFLIGHT` at once. When it is full, `publish()` returns −1
- When reconnecting with a persistent session (`setCleanSession(false)`, plus `setSessionExpiry()` for 5.0), unacknowledged PUBLISH and PUBREL packets are resent with DUP set. If the broker does not have the session (Session Present is 0), the local ones are discarded as well
- A PUBLISH larger than the receive buffer (`TINYMQ_MAX_PACKET`) is acknowledged and dropped (`droppedMessages()`). The connection is not closed. In 5.0 the client advertises Maximum Packet Size, so the broker does not send such packets

### Broker

- An incoming message goes into the queue (`TINYMQ_BROKER_MAX_QUEUED`) of each subscribed session, with up to `TINYMQ_BROKER_MAX_INFLIGHT` QoS 1 / 2 messages in flight (and, for 5.0, the client's Receive Maximum). To keep the order, once the limit is reached the messages behind wait too
- When a queue or the pool is full, messages are dropped (`droppedMessages()`)
- A 3.1.1 session gets one copy at the highest QoS even when subscriptions overlap. A 5.0 session gets one per subscription (because subscription identifiers and options differ per subscription)
- QoS 0 messages are not stored for disconnected sessions. When the session table is full, the oldest disconnected session is discarded. `setSessionExpiry()` sets an upper limit on the time
- Incoming QoS 2 is always accepted, by remembering only the packet ID and delivering on receipt. QoS 2 to subscribers can be dropped with `setMaxQos(1)` or `TINYMQ_ENABLE_QOS2_DELIVERY=0` (SUBACK grants a lower QoS)
- When a connection arrives with the same Client ID, the previous connection is closed (5.0: DISCONNECT 0x8E)

## 6. MQTT 5.0

### Client

```cpp
mqtt.setProtocolVersion(tinymq::kMqtt5);
mqtt.setSessionExpiry(3600);                 // keep the session for an hour after a disconnect

uint8_t wp[8];
tinymq::PropWriter w(wp, sizeof(wp));
w.u32(tinymq::kWillDelayInterval, 30);       // no will if it comes back within 30 s
mqtt.setWillProperties(w.result());

tinymq::SubOptions o;
o.qos = 1;
o.noLocal = true;                            // do not receive our own publishes
mqtt.subscribe("room/#", o, 7);              // subscription identifier 7
```

- To publish with properties, use `publish(topic, payload, len, qos, retain, props)`. Read the received properties from `Message::props` with `PropReader`
- The client follows the Receive Maximum, Maximum QoS, Retain Available, Maximum Packet Size and Server Keep Alive the broker sent in CONNACK (a publish beyond them returns −1). An assigned Client ID is in `assignedClientId()`
- `disconnect(tinymq::kDisconnectWithWill)` makes the broker send the will even on a disconnect
- A protocol violation found on this side is answered with a DISCONNECT carrying a reason code before the connection is closed. The reason for a DISCONNECT from the broker is in `onDisconnect` / `lastReason()`

### Broker

- Session and message expiry, Will Delay (cancelled if the client returns during the delay; sent if the session ends first)
- Subscription identifiers, No Local, Retain As Published, Retain Handling, shared subscriptions (`$share/<group>/<filter>`; delivered to online members in turn, with no retained messages)
- Topic aliases from clients (`TINYMQ_BROKER_TOPIC_ALIAS_MAX`; the broker does not use them)
- Reason codes: PUBACK 0x10 (no subscribers) / 0x87 (not authorized) / 0x99 (payload not UTF-8), SUBACK 0x8F / 0x87 / 0x97 / 0x9E, UNSUBACK 0x11, and DISCONNECT on protocol violations
- Enhanced authentication (AUTH) is not supported. A CONNECT with an Authentication Method is refused with 0x8C

## 7. MQTT-SN

### Gateway (`TinyMQSnListener`)

- Each MQTT-SN client is given one broker connection, and packets are translated both ways. QoS, retain, will, persistent sessions and authorization are the broker's own (Client IDs are shared with the broker too)
- Topic ID registrations are per client. When delivering to a wildcard subscription, or when the client has answered an unknown ID with "invalid topic ID", the gateway sends a REGISTER first
- Predefined topics (`addPredefinedTopic()`) and 2-character short topic names can be used without registering. QoS −1 (PUBLISH without a connection) is accepted only for these two
- **Sleep**: when a client puts a duration on its DISCONNECT, messages during that time (QoS 0 too) are held in the broker's queue, delivered on the wake-up PINGREQ, and then PINGRESP is returned
- **UDP losses**: unacknowledged deliveries are resent with DUP every Tretry (10 s by default); after more than Nretry (3 by default), the client is treated as disconnected and its will is sent (`setRetry()`)
- Source addresses are not authenticated. Use it **on a closed network or inside a TinyWG tunnel**

### Client (`TinyMQClientSn`)

| Level (`TINYMQ_SN_CLIENT_LEVEL`) | What it supports | Added on CH32V307 |
|---|---|---|
| 0 | QoS −1 sending only (`publishPredefined()`, `publishShort()`) | Flash +1.0 KB / RAM +0.4 KB |
| 1 | Connect, register, QoS 0 / 1 sending, subscribe, keepalive, retransmission | +5.1 KB / +1.2 KB |
| 2 (default) | Level 1 plus QoS 2, will, sleep, gateway discovery | +6.0 KB / +1.2 KB |

- Procedures wait for the gateway's response and resend every Tretry. Messages in flight are not copied and kept, so RAM is just the buffer and the topic table
- The topic ID table is not cleared on reconnect (a gateway the client returns to from sleep remembers the registrations). Stale mappings are fixed by a REGISTER from the gateway or an "invalid topic ID" reply
- Sleep: `sleep(seconds)` has the gateway hold messages, and `checkMessages()` now and then receives them. `connect()` ends the sleep

## 8. Bridge

```cpp
WiFiClient upstreamNet;
TinyMQBridge bridge(upstreamNet);

bridge.client().setServer("broker.example.com", 1883);
bridge.client().setClientId("site1");
bridge.client().setProtocolVersion(tinymq::kMqtt5);
bridge.addTopic("sensors/#", TinyMQBridge::kOut, 1, "", "site1/");   // sensors/x → site1/sensors/x
bridge.addTopic("cmd/#", TinyMQBridge::kIn, 1, "", "site1/");        // site1/cmd/x → cmd/x
broker.addListener(bridge);
```

- The mapping follows the same idea as mosquitto's `topic <pattern> <direction> <QoS> <local prefix> <remote prefix>`
- **Loops**: a bridge does not receive the messages it put into the broker itself. Upstream, with 5.0 it subscribes with No Local, so messages do not come back even with `kBoth`. If you use `kBoth` with 3.1.1, use different prefixes on the two sides
- The retain flag is kept when forwarding. On every connect, the bridge redoes the upstream subscriptions and the sending of local retained messages (so they come back even if upstream lost them)
- Outgoing messages while upstream is down are dropped and counted (`dropped()`). For what must reach upstream, make up for it with a persistent session on the upstream side, or with QoS and resending on the sending side
- The upstream connection starts with `connectAsync()`, so the broker does not stall while waiting for CONNACK (the TCP connection itself still waits as long as the Client's `connect()` does)

## 9. Authentication and authorization

```cpp
uint8_t onAuth(const tinymq::ConnectInfo &info, void *)
{
    if (info.hasUsername && info.username.equals("sensor") && info.hasPassword && info.password.equals("secret"))
        return tinymq::kSuccess;
    return tinymq::kBadUserNameOrPassword;    // becomes return code 4 in 3.1.1
}

bool onAcl(tinymq::Bytes clientId, tinymq::Bytes user, tinymq::Bytes topic, bool subscribe, void *)
{
    if (subscribe)
        return true;
    return topic.len >= 8 && memcmp(topic.data, "sensors/", 8) == 0;  // publish only under sensors/
}

broker.onAuthenticate(onAuth);
broker.onAuthorize(onAcl);
```

- An unauthorized publish is acknowledged and dropped in 3.1.1, and gets PUBACK 0x87 in 5.0. An unauthorized subscription fails in SUBACK (0x80 / 0x87)
- Publishes to topics starting with `$SYS` are not accepted from clients (acknowledged and dropped)
- The same authorization applies to MQTT-SN clients (decide by Client ID; there is no user name)

## 10. Persistence

`saveState()` writes out retained messages (with their remaining expiry) and sessions that have an expiry (subscriptions and undelivered QoS 1 / 2). Call `loadState()` after `begin()` and before registering listeners.

- QoS 1 / 2 messages that were in flight are resent from the start after loading. QoS 1 may be duplicated. The state of QoS 2 messages being received and delayed wills are not saved
- `onRetainedChange()` can trigger a save. Session changes (subscriptions, queues) are frequent, so saving at set intervals is the practical choice
- When saving to flash, write a new file and then swap it in, so a reset during the save leaves the previous state ([`Esp32Persist`](../examples/Esp32Persist/README.md))

## 11. Diagnosis

| What to check | What it tells you |
|---|---|
| `TinyMQClient::lastReason()` / `onDisconnect` | Why the connection ended (0x80: connection lost, 0x8D: keepalive timeout, 0x8E: taken over) |
| `TinyMQClient::droppedMessages()` | Messages dropped for being larger than the receive buffer |
| `TinyMQBroker::droppedMessages()` | Messages dropped because a queue, the pool or the retained table was full |
| `TinyMQBroker::poolUsed()` / `poolCapacity()` | Message pool usage |
| `clients()` / `sessions()` / `subscriptions()` / `retained()` | How full the tables are |
| `$SYS/broker/...` | `broker.setSysInterval(10)` publishes, retained, every 10 s: uptime, connections, sessions, subscriptions, retained messages, messages received / sent / dropped, pool use (the same names as mosquitto). Takes 11 retained slots |
| `TinyMQBridge::dropped()` | Messages dropped while upstream was down |
| `TinyMQClientSn::lastReturnCode()` | MQTT-SN return code |

## 12. Limitations

- The MQTT-SN gateway and client are IPv4 only
- MQTT 5.0 enhanced authentication (AUTH), TLS on the broker side, WebSocket and MQTT-SN 2.0 are out of scope
- The bridge does not store messages while upstream is down
- The broker does not use topic aliases itself (it only accepts them)
