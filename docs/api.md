# API reference

[日本語](api.ja.md) | [README](../README.md)

`#include <TinyMQ.h>` brings in everything. It splits into the Arduino classes (`TinyMQ...`) and the Arduino-independent protocol code (the `tinymq::` namespace). Unless stated otherwise, string arguments are **not copied** (keep them valid while connected; string literals are fine).

- [Common types](#common-types)
- [TinyMQClient](#tinymqclient)
- [TinyMQBroker](#tinymqbroker)
- [TinyMQTcpListener](#tinymqtcplistener)
- [TinyMQLwipListener](#tinymqlwiplistener)
- [TinyMQSnListener](#tinymqsnlistener)
- [TinyMQClientSn](#tinymqclientsn)
- [TinyMQBridge](#tinymqbridge)
- [Properties (MQTT 5.0)](#properties-mqtt-50)
- [Persistence](#persistence)
- [Reason codes](#reason-codes)

## Common types

### `tinymq::Bytes`

A reference to bytes owned elsewhere (`data`, `len`). Not NUL-terminated. `Bytes("text")` makes one from a string. Compare with `equals(Bytes)` / `equals(const char *)`.

### `tinymq::Message`

A received message. Valid only inside the callback.

| Member | Description |
|---|---|
| `const char *topic` / `uint16_t topicLen` | topic (NUL-terminated) |
| `const uint8_t *payload` / `size_t payloadLen` | payload |
| `uint8_t qos` | QoS it arrived with |
| `bool retain` / `bool dup` | retain flag, DUP flag |
| `Bytes props` | MQTT 5.0 properties (read with `PropReader`). Empty with 3.1.1 |

### `tinymq::SubOptions`

Subscription options. `qos` (0 to 2), `noLocal`, `retainAsPublished`, `retainHandling` (0: send retained messages every time, 1: only for a new subscription, 2: never). The last three are MQTT 5.0 only.

### Protocol versions

`tinymq::kMqtt31` (3), `tinymq::kMqtt311` (4), `tinymq::kMqtt5` (5).

## TinyMQClient

An MQTT client over any Arduino `Client`. Call `update()` from `loop()`.

```cpp
WiFiClient net;
TinyMQClient mqtt(net);
```

### Settings (used from the next `connect()`)

| Method | Description |
|---|---|
| `setServer(IPAddress ip, uint16_t port)` / `setServer(const char *host, uint16_t port)` | the broker |
| `setProtocolVersion(uint8_t version)` | `kMqtt311` (default) / `kMqtt5` / `kMqtt31` |
| `setClientId(const char *id)` | Client ID. If empty, the broker assigns one (with a clean session) |
| `setKeepAlive(uint16_t seconds)` | keepalive (default 60 s, 0 disables it) |
| `setCleanSession(bool clean)` | 3.1.1 clean session / 5.0 Clean Start (default true) |
| `setCredentials(const char *user, const char *password)` | user name and password. `nullptr` sends none |
| `setWill(topic, const char *payload, qos = 0, retain = false)` | will. Also a byte version, `setWill(topic, const uint8_t *, size_t, qos, retain)` |
| `clearWill()` | drops the will |
| `setConnectTimeout(uint32_t ms)` | how long to wait for CONNACK (default 10 s) |
| `setReconnectInterval(uint32_t ms)` | interval for reconnecting automatically after a drop. 0 (default) does not |
| `setSessionExpiry(uint32_t seconds)` | 5.0: how long the session is kept after a disconnect |
| `setConnectProperties(Bytes props)` | 5.0: properties added to CONNECT (user properties and others) |
| `setWillProperties(Bytes props)` | 5.0: will properties (Will Delay Interval and others) |

### Callbacks

Pass a function pointer and a `void *context`.

| Method | Called when |
|---|---|
| `onMessage(void (*)(const tinymq::Message &, void *))` | a message arrived |
| `onConnect(void (*)(uint8_t reason, bool sessionPresent, void *))` | a CONNACK was received (refusals included) |
| `onSubscribe(void (*)(uint16_t packetId, bool unsubscribe, const uint8_t *reasons, size_t count, void *))` | SUBACK / UNSUBACK |
| `onPublished(void (*)(uint16_t packetId, uint8_t reason, void *))` | a QoS 1 / 2 send finished (PUBACK / PUBCOMP) |
| `onDisconnect(void (*)(uint8_t reason, void *))` | the connection ended (`kSuccess` for `disconnect()`) |

### Operations

| Method | Description |
|---|---|
| `bool connect()` / `connect(const char *clientId)` | Connects and waits for CONNACK. true if accepted |
| `void connectAsync()` | Starts connecting and returns without waiting for CONNACK (`update()` handles the rest and keeps reconnecting) |
| `void disconnect(uint8_t reason = kSuccess)` | Disconnects. With 5.0, `kDisconnectWithWill` makes the broker publish the will |
| `bool connected()` | whether connected |
| `void update()` | Receiving, keepalive, reconnecting. Call from `loop()` |
| `int publish(topic, const char *payload, qos = 0, retain = false)` | Sends. Packet ID for QoS 1 / 2, 0 for QoS 0, −1 on failure |
| `int publish(topic, const uint8_t *payload, size_t len, qos = 0, retain = false)` | byte version |
| `int publish(topic, payload, len, qos, retain, Bytes props)` | 5.0: with properties (not Topic Alias or Subscription Identifier) |
| `int subscribe(const char *filter, uint8_t qos = 0)` | Subscribes. Packet ID or −1. The result comes to `onSubscribe` |
| `int subscribe(const char *filter, SubOptions opts, uint32_t subscriptionId = 0)` | 5.0: subscription options and subscription identifier |
| `int unsubscribe(const char *filter)` | Unsubscribes |

### State

| Method | Description |
|---|---|
| `uint8_t lastReason()` | why the connection last ended or was refused (5.0 reason code) |
| `size_t inflight()` | number of QoS 1 / 2 sends in progress |
| `uint32_t droppedMessages()` | number of messages dropped for being larger than the receive buffer |
| `const char *assignedClientId()` | 5.0: the Client ID the broker assigned ("" if none) |
| `tinymq::ClientEngine &engine()` | The protocol code itself. `serverReceiveMaximum()`, `serverMaximumPacketSize()`, `serverMaximumQos()`, `serverRetainAvailable()` and `keepAlive()` read what the CONNACK said |

Messages sent at QoS 1 / 2 are kept in the client's pool until acknowledged, and are sent again with DUP set when reconnecting with a persistent session.

## TinyMQBroker

An MQTT broker. Register listeners and call `update()` from `loop()`.

| Method | Description |
|---|---|
| `bool begin(size_t poolBytes = 0)` | Prepares. When `TINYMQ_BROKER_POOL_BYTES` is 0, allocates a pool of `poolBytes` here |
| `bool addListener(TinyMQListener &l)` | Registers a listener (TCP, MQTT-SN, bridge). Up to `TINYMQ_BROKER_MAX_LISTENERS` (4) |
| `void update()` | Runs every listener and the timers |
| `void end()` | Closes every connection (no wills) and clears sessions, subscriptions and retained messages |

### Settings

| Method | Description |
|---|---|
| `setMaxQos(uint8_t qos)` | highest QoS granted to subscribers (default 2). Incoming QoS 2 is always accepted |
| `setConnectTimeout(uint32_t ms)` | how long to wait from connecting to CONNECT (default 10 s) |
| `setRetainAvailable(bool on)` | false keeps no retained messages |
| `setSysInterval(uint32_t seconds)` | interval for publishing the state, retained, under `$SYS/broker/...`. 0 (default) publishes nothing and deletes what was published |
| `setSessionExpiry(uint32_t seconds)` | upper limit on how long a session outlives its connection. 0 (default) is no limit (when the session table is full, the oldest are dropped) |
| `onAuthenticate(uint8_t (*)(const tinymq::ConnectInfo &, void *))` | Authentication. `kSuccess` accepts; any other reason code refuses |
| `onAuthorize(bool (*)(Bytes clientId, Bytes username, Bytes topic, bool subscribe, void *))` | whether publish / subscribe is allowed per topic |
| `onClient(void (*)(Bytes clientId, bool connected, void *))` | clients connecting and disconnecting |
| `onRetainedChange(void (*)(Bytes topic, bool deleted, void *))` | a retained message was stored / deleted (a trigger for saving) |

`ConnectInfo` holds `clientId`, `hasUsername` / `username`, `hasPassword` / `password`, `version` and `props` (5.0).

### The sketch's own client

| Method | Description |
|---|---|
| `bool publish(topic, const char *payload, qos = 0, retain = false)` | Publishes inside the broker (a byte version also exists) |
| `bool subscribe(const char *filter, uint8_t qos = 0)` | Subscribes inside the broker. Retained messages arrive right away |
| `bool unsubscribe(const char *filter)` | Unsubscribes |
| `onMessage(void (*)(const tinymq::Message &, void *))` | what arrives for the sketch's subscriptions |

### Saving and loading

`saveState(StateSink &)` / `loadState(StateSource &)`. See [Persistence](#persistence).

### State

`clients()` (number connected), `sessions()` (including disconnected persistent sessions), `subscriptions()`, `retained()`, `poolUsed()` / `poolCapacity()`, `droppedMessages()` (number dropped because a queue or the pool was full), `receivedMessages()` / `sentMessages()`, `engine()` (`tinymq::Broker`).

## TinyMQTcpListener

A template that accepts TCP with an Arduino server (`WiFiServer`, `NetworkServer`, `EthernetServer` and others). The Client type comes from the server's `accept()` (or `available()` if there is none).

```cpp
WiFiServer server(1883);
TinyMQTcpListener<WiFiServer> listener(server);   // the second argument is the number of concurrent connections (default TINYMQ_BROKER_MAX_CLIENTS)
// server.begin(); broker.addListener(listener);
```

The sketch calls the server's `begin()`. Writes are left to the Client's `write()` (which may block), so with slow clients in the mix, `TinyMQLwipListener` is the better fit.

## TinyMQLwipListener

Accepts TCP on lwIP's raw API (ESP32, arduino-pico outside FreeRTOS mode, CH32V with the Network library). Available only when `TINYMQ_HAS_LWIP` is defined. On CH32V, `#include <Network.h>` must come first.

| Method | Description |
|---|---|
| `TinyMQLwipListener(uint16_t port)` | sets the port |
| `bool begin()` | Starts listening (once the network is up). false on failure |
| `void end()` | Closes every connection and the listening socket |

The lwIP callbacks only queue what is received; the broker runs inside `update()` holding the lwIP lock. For a client whose send buffer does not drain, the broker holds back delivery.

## TinyMQSnListener

An MQTT-SN 1.2 gateway built into the broker. The sketch calls the UDP's `begin()` (port 1884 by convention).

| Method | Description |
|---|---|
| `TinyMQSnListener(UDP &udp)` | sets the UDP |
| `bool addPredefinedTopic(uint16_t id, const char *name)` | Predefined topic, shared by every client. Up to `TINYMQ_SN_MAX_PREDEFINED` |
| `setGatewayId(uint8_t id)` | gateway ID for GWINFO / ADVERTISE (default 1) |
| `setAdvertise(uint16_t seconds, IPAddress broadcast, uint16_t port)` | interval for broadcasting ADVERTISE. 0 stops it |
| `setRetry(uint32_t ms, uint8_t count)` | Interval (Tretry, default 10 s) and count (Nretry, default 3) for resending unacknowledged messages. Past that, the client is treated as disconnected (its will is published) |
| `setQosMinusOne(bool on)` | whether to accept QoS −1 (PUBLISH without a connection) (default true) |
| `size_t clients()` | number of MQTT-SN clients |

## TinyMQClientSn

An MQTT-SN 1.2 client over any Arduino `UDP`. The sketch calls the UDP's `begin()` (any port). Exchanges wait for the gateway's reply (blocking). `TINYMQ_SN_CLIENT_LEVEL` decides which features are available.

| Method | Level | Description |
|---|---|---|
| `setGateway(IPAddress ip, uint16_t port = 1884)` | 0+ | the gateway |
| `onIdle(void (*)(void *), void *context)` | 0+ | called repeatedly while waiting for a reply |
| `bool publishPredefined(uint16_t id, payload, int8_t qos = -1, bool retain = false)` | 0+ | To a predefined topic. QoS −1 can be sent without connecting (a byte version also exists) |
| `bool publishShort(const char *name, payload, int8_t qos = -1, bool retain = false)` | 0+ | to a two-character short topic name |
| `setClientId` / `setKeepAlive` / `setCleanSession` | 1+ | connection settings |
| `setRetry(uint32_t ms, uint8_t count)` | 1+ | Tretry (default 10 s) and Nretry (default 3) |
| `onMessage(void (*)(const tinymq::SnMessage &, void *))` | 1+ | messages that arrive |
| `bool connect()` / `connect(const char *clientId)` | 1+ | connects |
| `void disconnect()` / `bool connected()` | 1+ | disconnects / state |
| `int registerTopic(const char *name)` | 1+ | registers a topic ID (publish registers automatically when needed) |
| `bool publish(const char *topic, payload, uint8_t qos = 0, bool retain = false)` | 1+ | Sends. Two characters are sent as a short topic name. QoS 2 needs level 2 |
| `int subscribe(const char *filter, uint8_t qos = 0)` | 1+ | Subscribes. The granted QoS or −1 |
| `int subscribePredefined(uint16_t id, uint8_t qos = 0)` | 1+ | subscribes to a predefined topic |
| `bool unsubscribe(const char *filter)` | 1+ | unsubscribes |
| `void update()` | 1+ | Receiving and keepalive. Call from `loop()` |
| `uint8_t lastReturnCode()` | 1+ | the last MQTT-SN return code (0 accepted, 1 congestion, 2 invalid topic ID, 3 not supported) |
| `setWill(topic, payload, qos = 0, retain = false)` / `clearWill()` | 2 | will |
| `bool sleep(uint16_t seconds)` / `bool asleep()` | 2 | declares sleep (the gateway holds messages) |
| `bool checkMessages()` | 2 | receives the messages held during sleep |
| `int searchGateway(IPAddress broadcast, port = 1884, radius = 1, timeoutMs = 5000)` | 2 | Sends SEARCHGW and uses the first gateway that answers. The gateway ID or −1 |

`SnMessage` holds `topic` (when the name is known; null for a predefined topic), `topicId`, `topicType` (0 registered, 1 predefined, 2 short name), `payload` / `payloadLen`, `qos` (−1 to 2), `retain` and `dup`.

## TinyMQBridge

A bridge from the broker to an upstream broker. Register it as a listener with `broker.addListener()`.

| Method | Description |
|---|---|
| `TinyMQBridge(Client &upstream)` | the Client used for the upstream connection (`WiFiClientSecure` for TLS) |
| `TinyMQClient &client()` | settings of the upstream connection (server, Client ID, credentials, version, will and so on) |
| `setReconnectInterval(uint32_t ms)` | reconnect interval (default 5 s) |
| `bool addTopic(pattern, Direction dir, uint8_t qos = 0, const char *localPrefix = "", const char *remotePrefix = "")` | Mapping table. The local side is `localPrefix + pattern`, the upstream side `remotePrefix + pattern`. `dir` is `kOut` (local to upstream) / `kIn` (upstream to local) / `kBoth`. Up to `TINYMQ_BRIDGE_MAX_TOPICS` |
| `bool connected()` | whether connected to the upstream |
| `forwardedOut()` / `forwardedIn()` / `dropped()` | number forwarded, number dropped (the upstream was down, the topic was too long) |

The QoS is the lower of the message's QoS and the mapping's QoS. On every connect, the upstream subscriptions and the sending of local retained messages are redone.

## Properties (MQTT 5.0)

Build with `tinymq::PropWriter` and read with `tinymq::PropReader`.

```cpp
uint8_t buf[64];
tinymq::PropWriter w(buf, sizeof(buf));
w.string(tinymq::kContentType, tinymq::Bytes("application/json"));
w.pair(tinymq::kUserProperty, tinymq::Bytes("site"), tinymq::Bytes("1"));
w.u32(tinymq::kMessageExpiryInterval, 60);
mqtt.publish("sensors/t", payload, len, 1, false, w.result());

tinymq::PropReader r(msg.props);
tinymq::Property p;
while (r.next(p)) { /* p.id, p.value, p.data, p.data2 */ }
```

| | |
|---|---|
| `PropWriter` | `byte(id, v)`, `u16(id, v)`, `u32(id, v)`, `varint(id, v)`, `string(id, Bytes)` (binary too), `pair(id, name, value)`, `copy(Bytes props, skip, count)`, `ok()`, `result()` |
| `PropReader` | `next(Property &)`, `ok()` |
| `Property` | `id`, `value` (integer), `data` (string / binary / user property name), `data2` (user property value) |
| `findProp(Bytes props, uint8_t id, Property &out)` | finds the first `id` |
| `checkProps(Bytes props, uint16_t scope)` | checks type, range, where each may appear, and duplicates |

IDs are `tinymq::kPayloadFormatIndicator`, `kMessageExpiryInterval`, `kContentType`, `kResponseTopic`, `kCorrelationData`, `kSubscriptionIdentifier`, `kSessionExpiryInterval`, `kUserProperty` and others (`src/tinymq/mq_props.h`).

## Persistence

`TinyMQBroker::saveState(tinymq::StateSink &)` writes out retained messages and persistent sessions (subscriptions, undelivered QoS 1 / 2). `loadState(tinymq::StateSource &)` reads them back before clients connect. The sketch provides the storage.

```cpp
struct FileSink : tinymq::StateSink {
    File f;
    bool write(const uint8_t *d, size_t n) override { return f.write(d, n) == n; }
};
struct FileSource : tinymq::StateSource {
    File f;
    bool read(uint8_t *d, size_t n) override { return f.read(d, n) == (int)n; }  // exactly n bytes
};
```

Example: [`Esp32Persist`](../examples/Esp32Persist/README.md).

## Reason codes

Values of `tinymq::Reason` (MQTT 5.0 reason codes; used internally with 3.1.1 too). The common ones:

| Value | Name | |
|---|---|---|
| 0x00 | `kSuccess` | success |
| 0x04 | `kDisconnectWithWill` | disconnect and publish the will |
| 0x10 | `kNoMatchingSubscribers` | no subscribers (PUBACK) |
| 0x80 | `kUnspecifiedError` | the connection dropped, and others |
| 0x81 / 0x82 | `kMalformedPacket` / `kProtocolError` | protocol violation |
| 0x84 | `kUnsupportedProtocolVersion` | version not supported |
| 0x85 | `kClientIdentifierNotValid` | Client ID not accepted |
| 0x86 / 0x87 | `kBadUserNameOrPassword` / `kNotAuthorized` | authentication / authorization failed |
| 0x8D | `kKeepAliveTimeout` | keepalive expired |
| 0x8E | `kSessionTakenOver` | taken over by a connection with the same Client ID |
| 0x95 | `kPacketTooLarge` | packet too large |

They are mapped to and from 3.1.1 CONNACK return codes automatically.
