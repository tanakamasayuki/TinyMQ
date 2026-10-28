# API リファレンス

[English](api.md) | [README](../README.ja.md)

`#include <TinyMQ.h>` ですべてが使えます。Arduino 側のクラス(`TinyMQ...`)と、Arduino に依存しないプロトコルの処理(`tinymq::` の名前空間)に分かれます。文字列の引数は、特に書いていない限り**コピーされません**(つながっている間は有効なままにしてください。文字列リテラルなら問題ありません)。

- [共通の型](#共通の型)
- [TinyMQClient](#tinymqclient)
- [TinyMQBroker](#tinymqbroker)
- [TinyMQTcpListener](#tinymqtcplistener)
- [TinyMQLwipListener](#tinymqlwiplistener)
- [TinyMQSnListener](#tinymqsnlistener)
- [TinyMQClientSn](#tinymqclientsn)
- [TinyMQBridge](#tinymqbridge)
- [プロパティ(MQTT 5.0)](#プロパティmqtt-50)
- [永続化](#永続化)
- [理由コード](#理由コード)

## 共通の型

### `tinymq::Bytes`

ほかが持っているバイト列への参照(`data`、`len`)。NUL 終端ではありません。`Bytes("text")` で文字列から作れます。`equals(Bytes)` / `equals(const char *)` で比べます。

### `tinymq::Message`

受信したメッセージ。コールバックの中だけ有効です。

| メンバ | 内容 |
|---|---|
| `const char *topic` / `uint16_t topicLen` | トピック(NUL 終端) |
| `const uint8_t *payload` / `size_t payloadLen` | ペイロード |
| `uint8_t qos` | 届いた QoS |
| `bool retain` / `bool dup` | retain フラグ、DUP フラグ |
| `Bytes props` | MQTT 5.0 のプロパティ(`PropReader` で読む)。3.1.1 では空 |

### `tinymq::SubOptions`

購読オプション。`qos`(0〜2)、`noLocal`、`retainAsPublished`、`retainHandling`(0: 毎回 retain を送る、1: 新しい購読のときだけ、2: 送らない)。後の 3 つは MQTT 5.0 のみ。

### プロトコルのバージョン

`tinymq::kMqtt31`(3)、`tinymq::kMqtt311`(4)、`tinymq::kMqtt5`(5)。

## TinyMQClient

任意の Arduino `Client` の上で動く MQTT クライアント。`loop()` から `update()` を呼びます。

```cpp
WiFiClient net;
TinyMQClient mqtt(net);
```

### 設定(次の `connect()` から使われる)

| メソッド | 内容 |
|---|---|
| `setServer(IPAddress ip, uint16_t port)` / `setServer(const char *host, uint16_t port)` | ブローカー |
| `setProtocolVersion(uint8_t version)` | `kMqtt311`(既定)/ `kMqtt5` / `kMqtt31` |
| `setClientId(const char *id)` | Client ID。空ならブローカーが割り当てる(クリーンセッションのとき) |
| `setKeepAlive(uint16_t seconds)` | keepalive(既定 60 秒、0 で無効) |
| `setCleanSession(bool clean)` | 3.1.1 のクリーンセッション / 5.0 の Clean Start(既定 true) |
| `setCredentials(const char *user, const char *password)` | ユーザ名とパスワード。`nullptr` で送らない |
| `setWill(topic, const char *payload, qos = 0, retain = false)` | will。バイト列版 `setWill(topic, const uint8_t *, size_t, qos, retain)` も |
| `clearWill()` | will をやめる |
| `setConnectTimeout(uint32_t ms)` | CONNACK を待つ時間(既定 10 秒) |
| `setReconnectInterval(uint32_t ms)` | 切れたら自動でつなぎ直す間隔。0(既定)はしない |
| `setSessionExpiry(uint32_t seconds)` | 5.0: 切れたあとセッションを保つ時間 |
| `setConnectProperties(Bytes props)` | 5.0: CONNECT に足すプロパティ(ユーザープロパティなど) |
| `setWillProperties(Bytes props)` | 5.0: will のプロパティ(Will Delay Interval など) |

### コールバック

関数ポインタと `void *context` を渡します。

| メソッド | 呼ばれるとき |
|---|---|
| `onMessage(void (*)(const tinymq::Message &, void *))` | メッセージが届いた |
| `onConnect(void (*)(uint8_t reason, bool sessionPresent, void *))` | CONNACK を受けた(拒否も含む) |
| `onSubscribe(void (*)(uint16_t packetId, bool unsubscribe, const uint8_t *reasons, size_t count, void *))` | SUBACK / UNSUBACK |
| `onPublished(void (*)(uint16_t packetId, uint8_t reason, void *))` | QoS 1 / 2 の送信が終わった(PUBACK / PUBCOMP) |
| `onDisconnect(void (*)(uint8_t reason, void *))` | 接続が終わった(`disconnect()` なら `kSuccess`) |

### 操作

| メソッド | 内容 |
|---|---|
| `bool connect()` / `connect(const char *clientId)` | 接続して CONNACK を待つ。受け入れられたら true |
| `void connectAsync()` | 接続を始めて、CONNACK を待たずに戻る(以後 `update()` が処理し、再接続も続ける) |
| `void disconnect(uint8_t reason = kSuccess)` | 切断。5.0 で `kDisconnectWithWill` なら will を出させる |
| `bool connected()` | つながっているか |
| `void update()` | 受信、keepalive、再接続。`loop()` から呼ぶ |
| `int publish(topic, const char *payload, qos = 0, retain = false)` | 送信。QoS 1 / 2 はパケット ID、QoS 0 は 0、失敗は −1 |
| `int publish(topic, const uint8_t *payload, size_t len, qos = 0, retain = false)` | バイト列版 |
| `int publish(topic, payload, len, qos, retain, Bytes props)` | 5.0: プロパティ付き(Topic Alias と Subscription Identifier は不可) |
| `int subscribe(const char *filter, uint8_t qos = 0)` | 購読。パケット ID か −1。結果は `onSubscribe` |
| `int subscribe(const char *filter, SubOptions opts, uint32_t subscriptionId = 0)` | 5.0: 購読オプションと購読識別子 |
| `int unsubscribe(const char *filter)` | 購読解除 |

### 状態

| メソッド | 内容 |
|---|---|
| `uint8_t lastReason()` | 最後に接続が終わった / 拒否された理由(5.0 の理由コード) |
| `size_t inflight()` | 送信中の QoS 1 / 2 の数 |
| `uint32_t droppedMessages()` | 受信バッファより大きくて捨てたメッセージの数 |
| `const char *assignedClientId()` | 5.0: ブローカーが割り当てた Client ID(なければ "") |
| `tinymq::ClientEngine &engine()` | プロトコルの処理そのもの。`serverReceiveMaximum()`、`serverMaximumPacketSize()`、`serverMaximumQos()`、`serverRetainAvailable()`、`keepAlive()` で CONNACK の内容が読める |

QoS 1 / 2 で送ったメッセージは、確認が来るまでクライアントのプールに保持し、永続セッションでつなぎ直したときに DUP 付きで送り直します。

## TinyMQBroker

MQTT ブローカー。受け口(listener)を登録し、`loop()` から `update()` を呼びます。

| メソッド | 内容 |
|---|---|
| `bool begin(size_t poolBytes = 0)` | 準備。`TINYMQ_BROKER_POOL_BYTES` が 0 のときは、ここでプールを `poolBytes` 確保する |
| `bool addListener(TinyMQListener &l)` | 受け口(TCP、MQTT-SN、ブリッジ)を登録。最大 `TINYMQ_BROKER_MAX_LISTENERS`(4) |
| `void update()` | すべての受け口とタイマーを動かす |
| `void end()` | すべての接続を閉じ(will は出さない)、セッション・購読・retain を消す |

### 設定

| メソッド | 内容 |
|---|---|
| `setMaxQos(uint8_t qos)` | 購読者に許す最大の QoS(既定 2)。受信の QoS 2 は常に受け付ける |
| `setConnectTimeout(uint32_t ms)` | 接続から CONNECT までの待ち時間(既定 10 秒) |
| `setRetainAvailable(bool on)` | false で retain を保持しない |
| `setSysInterval(uint32_t seconds)` | `$SYS/broker/...` に状態を retain で出す間隔。0(既定)で出さず、出したものを消す |
| `setSessionExpiry(uint32_t seconds)` | セッションが接続より長く残る上限。0(既定)は上限なし(セッション表が埋まったら古いものから捨てる) |
| `onAuthenticate(uint8_t (*)(const tinymq::ConnectInfo &, void *))` | 認証。`kSuccess` で受け入れ、ほかの理由コードで拒否 |
| `onAuthorize(bool (*)(Bytes clientId, Bytes username, Bytes topic, bool subscribe, void *))` | トピックごとの publish / subscribe の可否 |
| `onClient(void (*)(Bytes clientId, bool connected, void *))` | クライアントの接続と切断 |
| `onRetainedChange(void (*)(Bytes topic, bool deleted, void *))` | retain が保存 / 削除された(保存のきっかけに) |

`ConnectInfo` は `clientId`、`hasUsername` / `username`、`hasPassword` / `password`、`version`、`props`(5.0)を持ちます。

### スケッチ自身のクライアント

| メソッド | 内容 |
|---|---|
| `bool publish(topic, const char *payload, qos = 0, retain = false)` | ブローカーの中で送信(バイト列版もある) |
| `bool subscribe(const char *filter, uint8_t qos = 0)` | ブローカーの中で購読。retain はすぐ届く |
| `bool unsubscribe(const char *filter)` | 購読解除 |
| `onMessage(void (*)(const tinymq::Message &, void *))` | スケッチの購読に届いたもの |

### 保存と読み戻し

`saveState(StateSink &)` / `loadState(StateSource &)`。[永続化](#永続化)を参照。

### 状態

`clients()`(つながっている数)、`sessions()`(切断中の永続セッションを含む)、`subscriptions()`、`retained()`、`poolUsed()` / `poolCapacity()`、`droppedMessages()`(キューやプールが一杯で捨てた数)、`receivedMessages()` / `sentMessages()`、`engine()`(`tinymq::Broker`)。

## TinyMQTcpListener

Arduino のサーバー(`WiFiServer`、`NetworkServer`、`EthernetServer` など)で TCP を受けるテンプレート。サーバーの `accept()`(なければ `available()`)から Client の型を決めます。

```cpp
WiFiServer server(1883);
TinyMQTcpListener<WiFiServer> listener(server);   // 第 2 引数で同時接続数(既定 TINYMQ_BROKER_MAX_CLIENTS)
// server.begin(); broker.addListener(listener);
```

サーバーの `begin()` はスケッチが呼びます。書き込みは Client の `write()` に任せる(ブロックしうる)ので、遅いクライアントが混じる場合は `TinyMQLwipListener` の方が向きます。

## TinyMQLwipListener

lwIP の raw API で TCP を受けます(ESP32、arduino-pico の非 FreeRTOS モード、CH32V の Network ライブラリ)。`TINYMQ_HAS_LWIP` が定義されるときだけ使えます。CH32V では先に `#include <Network.h>` が必要です。

| メソッド | 内容 |
|---|---|
| `TinyMQLwipListener(uint16_t port)` | ポートを指定 |
| `bool begin()` | 待ち受けを始める(ネットワークがつながってから)。失敗で false |
| `void end()` | すべての接続と待ち受けを閉じる |

lwIP のコールバックは受信を溜めるだけで、ブローカーは `update()` の中で lwIP のロックを取って動きます。送信バッファが空かないクライアントには、ブローカーが配送を待たせます。

## TinyMQSnListener

ブローカーに組み込む MQTT-SN 1.2 のゲートウェイ。UDP の `begin()` はスケッチが呼びます(1884 番が慣例)。

| メソッド | 内容 |
|---|---|
| `TinyMQSnListener(UDP &udp)` | UDP を指定 |
| `bool addPredefinedTopic(uint16_t id, const char *name)` | 事前定義トピック。全クライアント共通。最大 `TINYMQ_SN_MAX_PREDEFINED` |
| `setGatewayId(uint8_t id)` | GWINFO / ADVERTISE のゲートウェイ ID(既定 1) |
| `setAdvertise(uint16_t seconds, IPAddress broadcast, uint16_t port)` | ADVERTISE をブロードキャストする間隔。0 で止める |
| `setRetry(uint32_t ms, uint8_t count)` | 確認のないメッセージを送り直す間隔(Tretry、既定 10 秒)と回数(Nretry、既定 3)。超えたらクライアントは切断扱い(will が出る) |
| `setQosMinusOne(bool on)` | QoS −1(接続なしの PUBLISH)を受け付けるか(既定 true) |
| `size_t clients()` | MQTT-SN のクライアント数 |

## TinyMQClientSn

任意の Arduino `UDP` の上で動く MQTT-SN 1.2 クライアント。UDP の `begin()` はスケッチが呼びます(ポートは何番でも)。手順はゲートウェイの応答を待ちます(ブロッキング)。使える機能は `TINYMQ_SN_CLIENT_LEVEL` で決まります。

| メソッド | 段階 | 内容 |
|---|---|---|
| `setGateway(IPAddress ip, uint16_t port = 1884)` | 0〜 | ゲートウェイ |
| `onIdle(void (*)(void *), void *context)` | 0〜 | 応答を待つ間に繰り返し呼ばれる |
| `bool publishPredefined(uint16_t id, payload, int8_t qos = -1, bool retain = false)` | 0〜 | 事前定義トピックへ。QoS −1 は接続なしで送れる(バイト列版もある) |
| `bool publishShort(const char *name, payload, int8_t qos = -1, bool retain = false)` | 0〜 | 2 文字の短いトピック名へ |
| `setClientId` / `setKeepAlive` / `setCleanSession` | 1〜 | 接続の設定 |
| `setRetry(uint32_t ms, uint8_t count)` | 1〜 | Tretry(既定 10 秒)と Nretry(既定 3) |
| `onMessage(void (*)(const tinymq::SnMessage &, void *))` | 1〜 | 届いたメッセージ |
| `bool connect()` / `connect(const char *clientId)` | 1〜 | 接続 |
| `void disconnect()` / `bool connected()` | 1〜 | 切断 / 状態 |
| `int registerTopic(const char *name)` | 1〜 | トピック ID の登録(publish は必要なら自動で登録する) |
| `bool publish(const char *topic, payload, uint8_t qos = 0, bool retain = false)` | 1〜 | 送信。2 文字なら短いトピック名で送る。QoS 2 は段階 2 |
| `int subscribe(const char *filter, uint8_t qos = 0)` | 1〜 | 購読。許された QoS か −1 |
| `int subscribePredefined(uint16_t id, uint8_t qos = 0)` | 1〜 | 事前定義トピックを購読 |
| `bool unsubscribe(const char *filter)` | 1〜 | 購読解除 |
| `void update()` | 1〜 | 受信と keepalive。`loop()` から呼ぶ |
| `uint8_t lastReturnCode()` | 1〜 | 最後の MQTT-SN の戻りコード(0 受け入れ、1 混雑、2 不正なトピック ID、3 未対応) |
| `setWill(topic, payload, qos = 0, retain = false)` / `clearWill()` | 2 | will |
| `bool sleep(uint16_t seconds)` / `bool asleep()` | 2 | スリープを宣言する(ゲートウェイがメッセージを預かる) |
| `bool checkMessages()` | 2 | スリープ中に預けたメッセージを受け取る |
| `int searchGateway(IPAddress broadcast, port = 1884, radius = 1, timeoutMs = 5000)` | 2 | SEARCHGW を送り、最初に答えたゲートウェイを使う。ゲートウェイ ID か −1 |

`SnMessage` は `topic`(名前が分かるとき。事前定義なら null)、`topicId`、`topicType`(0 登録済み、1 事前定義、2 短い名前)、`payload` / `payloadLen`、`qos`(−1〜2)、`retain`、`dup`。

## TinyMQBridge

ブローカーから上流のブローカーへのブリッジ。受け口として `broker.addListener()` に登録します。

| メソッド | 内容 |
|---|---|
| `TinyMQBridge(Client &upstream)` | 上流への接続に使う Client(TLS なら `WiFiClientSecure`) |
| `TinyMQClient &client()` | 上流の接続の設定(サーバー、Client ID、資格情報、バージョン、will など) |
| `setReconnectInterval(uint32_t ms)` | 再接続の間隔(既定 5 秒) |
| `bool addTopic(pattern, Direction dir, uint8_t qos = 0, const char *localPrefix = "", const char *remotePrefix = "")` | 対応表。ローカル側は `localPrefix + pattern`、上流側は `remotePrefix + pattern`。`dir` は `kOut`(ローカル → 上流)/ `kIn`(上流 → ローカル)/ `kBoth`。最大 `TINYMQ_BRIDGE_MAX_TOPICS` |
| `bool connected()` | 上流とつながっているか |
| `forwardedOut()` / `forwardedIn()` / `dropped()` | 転送した数、捨てた数(上流が落ちていた、トピックが長すぎた) |

QoS は、メッセージの QoS と対応表の QoS の小さい方です。つながるたびに、上流の購読とローカルの retain の送信をやり直します。

## プロパティ(MQTT 5.0)

`tinymq::PropWriter` で組み立て、`tinymq::PropReader` で読みます。

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
| `PropWriter` | `byte(id, v)`、`u16(id, v)`、`u32(id, v)`、`varint(id, v)`、`string(id, Bytes)`(バイナリも)、`pair(id, name, value)`、`copy(Bytes props, skip, count)`、`ok()`、`result()` |
| `PropReader` | `next(Property &)`、`ok()` |
| `Property` | `id`、`value`(整数)、`data`(文字列 / バイナリ / ユーザープロパティの名前)、`data2`(ユーザープロパティの値) |
| `findProp(Bytes props, uint8_t id, Property &out)` | 最初の `id` を探す |
| `checkProps(Bytes props, uint16_t scope)` | 型、範囲、置ける場所、重複を確かめる |

ID は `tinymq::kPayloadFormatIndicator`、`kMessageExpiryInterval`、`kContentType`、`kResponseTopic`、`kCorrelationData`、`kSubscriptionIdentifier`、`kSessionExpiryInterval`、`kUserProperty` など(`src/tinymq/mq_props.h`)。

## 永続化

`TinyMQBroker::saveState(tinymq::StateSink &)` は、retain と永続セッション(購読、未配送の QoS 1 / 2)を書き出します。`loadState(tinymq::StateSource &)` は、クライアントがつながる前に読み戻します。保存先はスケッチが用意します。

```cpp
struct FileSink : tinymq::StateSink {
    File f;
    bool write(const uint8_t *d, size_t n) override { return f.write(d, n) == n; }
};
struct FileSource : tinymq::StateSource {
    File f;
    bool read(uint8_t *d, size_t n) override { return f.read(d, n) == (int)n; }  // ちょうど n バイト
};
```

例は [`Esp32Persist`](../examples/Esp32Persist/README.ja.md)。

## 理由コード

`tinymq::Reason` の値(MQTT 5.0 の理由コード。3.1.1 でも内部ではこれを使います)。よく使うもの:

| 値 | 名前 | |
|---|---|---|
| 0x00 | `kSuccess` | 成功 |
| 0x04 | `kDisconnectWithWill` | will を出して切断 |
| 0x10 | `kNoMatchingSubscribers` | 購読者がいない(PUBACK) |
| 0x80 | `kUnspecifiedError` | 接続が切れた、など |
| 0x81 / 0x82 | `kMalformedPacket` / `kProtocolError` | プロトコル違反 |
| 0x84 | `kUnsupportedProtocolVersion` | バージョン未対応 |
| 0x85 | `kClientIdentifierNotValid` | Client ID を受け付けない |
| 0x86 / 0x87 | `kBadUserNameOrPassword` / `kNotAuthorized` | 認証・認可の失敗 |
| 0x8D | `kKeepAliveTimeout` | keepalive 切れ |
| 0x8E | `kSessionTakenOver` | 同じ Client ID の接続に取って代わられた |
| 0x95 | `kPacketTooLarge` | パケットが大きすぎる |

3.1.1 の CONNACK の戻りコードとは自動で読み替えます。
