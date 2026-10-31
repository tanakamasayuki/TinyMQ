# 上級者ガイド

[English](guide-advanced.md) | [README](../README.ja.md) | [初心者ガイド](guide-beginner.ja.md) | [API リファレンス](api.ja.md)

TinyMQ の作りと、設定、使い分け、注意点をまとめます。使い方の基本は[初心者ガイド](guide-beginner.ja.md)にあります。

## 1. 構成

```
スケッチ
 ├ TinyMQClient        MQTT 3.1.1 / 5.0 クライアント(Arduino Client の上)
 ├ TinyMQClientSn      MQTT-SN 1.2 クライアント(Arduino UDP の上)
 └ TinyMQBroker        ブローカー + スケッチ自身の購読 + タップ
        ├ TinyMQTcpListener<Server>   Arduino のサーバーで TCP を受ける
        ├ TinyMQLwipListener          lwIP の raw API で TCP を受ける
        ├ TinyMQSnListener            MQTT-SN のゲートウェイ(Arduino UDP)
        └ TinyMQBridge                上流のブローカーへのブリッジ(中に TinyMQClient)
tinymq::               プロトコルの処理(Arduino にも lwIP にも依存しない)
  ClientEngine / Broker / SnGateway / SnClientEngine
  コーデック(3.1.1 / 5.0 / MQTT-SN)、プロパティ、トピックの照合、メッセージプール
```

Arduino 側のクラスは、`tinymq::` のエンジンと Arduino の `Client` / `Server` / `UDP` / lwIP をつなぐ薄いアダプタです。`engine()` でエンジンそのものに触れます。ほかの通信路(独自のモジュール、別の TCP/IP スタック)で使うときは、エンジンに直接バイト列を渡せます(`tinymq::Link` を実装し、`rxBuffer()` / `rxCommit()` で受信を渡す)。

ブローカーの中では、TCP のクライアント、MQTT-SN のクライアント、スケッチ自身、ブリッジが、同じトピック空間とセッション表を共有します。MQTT-SN のクライアントは、ゲートウェイがブローカーの接続 1 本に読み替えています(§7)。

## 2. メモリと設定

### 動的確保をしない

テーブルとバッファは、すべてクラスの中に静的に取ります。可変長のもの(retain、配送待ちのメッセージ、will、Client ID、購読のフィルタ)はブローカーの**メッセージプール**に置きます。1 つのメッセージを複数の購読者へ配るときは、本文を参照カウントで共有し、コピーしません。

`TINYMQ_BROKER_POOL_BYTES` を 0 にすると、プールを `begin(バイト数)` で 1 回だけ確保します(ESP32 の PSRAM を使うときなど)。

### 設定(ビルド時、`-D` で上書き)

| マクロ | 既定値 | 内容 |
|---|---|---|
| `TINYMQ_MAX_TOPIC_LEN` | 128 | トピック名の最大長 |
| `TINYMQ_MAX_PACKET` | 1024 | クライアントの受信 / 送信バッファ。ブローカーの受信バッファの既定 |
| `TINYMQ_CLIENT_POOL_BYTES` | 2048 | クライアントが送信中の QoS 1 / 2 を保持する領域 |
| `TINYMQ_CLIENT_MAX_INFLIGHT` | 8 | クライアントが同時に送信中にできる QoS 1 / 2 |
| `TINYMQ_CLIENT_MAX_QOS2_IN` | 8 | クライアントが受信中の QoS 2(重複の検出) |
| `TINYMQ_BROKER_MAX_CLIENTS` | 8 | ブローカーの同時接続(TCP と MQTT-SN の合計) |
| `TINYMQ_BROKER_MAX_SESSIONS` | 16 | セッション(切断中の永続セッションを含む) |
| `TINYMQ_BROKER_MAX_SUBSCRIPTIONS` | 64 | 購読の総数(スケッチとブリッジの分も) |
| `TINYMQ_BROKER_MAX_RETAINED` | 32 | retain の数 |
| `TINYMQ_BROKER_MAX_QUEUED` | 32 | セッションごとの配送待ち(送信中を含む) |
| `TINYMQ_BROKER_MAX_INFLIGHT` | 8 | セッションごとの送信中の QoS 1 / 2 |
| `TINYMQ_BROKER_MAX_QOS2_IN` | 16 | セッションごとの受信中の QoS 2。5.0 の Receive Maximum として伝える |
| `TINYMQ_BROKER_RX_BUFFER` | `TINYMQ_MAX_PACKET` | 接続ごとの受信バッファ = クライアントが送れる最大のパケット |
| `TINYMQ_BROKER_POOL_BYTES` | 16384 | メッセージプール(0: `begin()` で確保) |
| `TINYMQ_BROKER_MAX_LISTENERS` | 4 | 受け口の数 |
| `TINYMQ_BROKER_MAX_TAPS` | 2 | ブリッジの数 |
| `TINYMQ_BROKER_TOPIC_ALIAS_MAX` | 8 | 5.0 のクライアントが使えるトピックエイリアス(0: 使わせない) |
| `TINYMQ_AUTH_DATA` | 128 | 拡張認証のハンドラが返せる Authentication Data の長さ(クライアントとブローカー) |
| `TINYMQ_ENABLE_MQTT5` | 1 | ブローカーで 5.0 を受け付ける |
| `TINYMQ_ENABLE_SHARED_SUB` | 1 | 共有購読 |
| `TINYMQ_ENABLE_QOS2_DELIVERY` | 1 | 購読者への QoS 2 の配送 |
| `TINYMQ_SN_MAX_CLIENTS` | 8 | MQTT-SN のクライアント(それぞれブローカーの接続を 1 本使う) |
| `TINYMQ_SN_MAX_TOPICS` | 64 | トピック ID の登録の総数 |
| `TINYMQ_SN_MAX_PREDEFINED` | 16 | 事前定義トピック |
| `TINYMQ_SN_POOL_BYTES` | 4096 | 登録したトピック名の領域 |
| `TINYMQ_SN_MAX_NODE_ID` | 8 | フォワーダーの後ろの Wireless Node Id の長さ |
| `TINYMQ_SN_CLIENT_LEVEL` | 2 | MQTT-SN クライアントの段階(§7) |
| `TINYMQ_SN_CLIENT_BUFFER` | 256 | MQTT-SN クライアントのデータグラムの最大長 |
| `TINYMQ_SN_CLIENT_MAX_TOPICS` | 8 | MQTT-SN クライアントが覚えるトピック ID |
| `TINYMQ_SN_CLIENT_TOPIC_LEN` | 32 | そのトピック名の最大長 |
| `TINYMQ_BRIDGE_MAX_TOPICS` | 8 | ブリッジごとの対応表 |
| `TINYMQ_BRIDGE_QUEUE_BYTES` | 2048 | 上流が落ちている間に預かる QoS 1 / 2 の領域(0: 預からない) |
| `TINYMQ_BRIDGE_MAX_QUEUED` | 16 | その件数 |

**プリセット**: `-DTINYMQ_PRESET_SMALL` を付けると、ネットワークのスタックで RAM が残りにくいボード(Ethernet の CH32V307 など)向けに、既定値をまとめて小さくします。接続数と最大のパケットは変えず、セッション 8、購読 32、retain 16、配送待ち 16、受信中の QoS 2 を 8、プール 8192、MQTT-SN のクライアント 4、登録 32、その領域 2048 にします。個々の値はこれまでどおり `-D` で上書きできます。CH32V307(Flash / RAM 192K / 128K)では、ブローカーが 37.7 KB から 21.8 KB に、ゲートウェイが 10.2 KB から 7.3 KB になります。

Arduino IDE は `-D` を渡す場所がありません。arduino-cli なら `--build-property "compiler.cpp.extra_flags=-DTINYMQ_BROKER_MAX_CLIENTS=4"`、PlatformIO なら `build_flags` で渡します。ライブラリ全体に効くので、スケッチの中の `#define` では変わりません。

### 見積もり

ブローカーの RAM は、おおよそ次の合計です(32 ビットのマイコン)。

- 接続: `MAX_CLIENTS ×(RX_BUFFER + 約 100)`
- セッション: `MAX_SESSIONS ×(約 40 + MAX_QUEUED × 16 + MAX_QOS2_IN × 2)`
- 購読: `MAX_SUBSCRIPTIONS × 約 24`、retain: `MAX_RETAINED × 4`
- メッセージプール: `POOL_BYTES`、送信バッファ: `MAX_PACKET`
- MQTT-SN のゲートウェイを使うとき: 約 `3 × MAX_PACKET + SN_POOL_BYTES + SN_MAX_TOPICS × 12`

既定の設定では ESP32 で約 48 KB(ゲートウェイ込み)です。CH32V307 で `MAX_CLIENTS=4 MAX_SESSIONS=6 POOL_BYTES=4096 MAX_PACKET=512 MAX_QUEUED=8` にすると、lwIP を含めて RAM 47 KB に収まります。

使わない機能(5.0、MQTT-SN、ブリッジ)のコードは、呼ばなければリンカが落とします。

## 3. 受け口の選び方

| 受け口 | 向いているもの |
|---|---|
| `TinyMQLwipListener` | lwIP のあるコア(ESP32、Pico W、CH32V)。ソケットを使わず軽い。TCP の送信バッファの空きを見て配送を待たせるので、読むのが遅いクライアントがいてもブローカーが止まらない |
| `TinyMQTcpListener<Server>` | lwIP のないネットワーク(W5500 の Ethernet ライブラリなど)や、host。Arduino の `Client::write()` に任せるので、相手が受け取らないと書き込みで待つことがある |
| `TinyMQSnListener` | MQTT-SN のクライアント |
| `TinyMQBridge` | 上流のブローカーとの間の転送 |

**遅延 ACK**: Nagle のアルゴリズムを止めていないクライアント(paho-mqtt など多くのもの)は、前に送ったものの ACK が来るまで次の小さなパケットを送りません。返事のない QoS 0 では、ブローカー側の遅延 ACK(lwIP では最大 250 ms)の分だけ遅れます。`TinyMQLwipListener` は受け取るたびにすぐ ACK を返すので、この遅れがありません。`TinyMQTcpListener` では ACK を制御できないので、クライアント側で Nagle を止める(`TCP_NODELAY`)か、QoS 1 を使うと遅れません。

複数を同時に登録できます(例: Wi-Fi は lwIP、W5500 は `TinyMQTcpListener`)。接続数の上限 `TINYMQ_BROKER_MAX_CLIENTS` は全部の合計です。

## 4. 実行モデル

- すべて **`loop()` から呼ぶポーリング型**です。`TinyMQClient::update()`、`TinyMQBroker::update()`、`TinyMQClientSn::update()` を呼び続けてください
- **待つ場所**: `TinyMQClient::connect()`(TCP の接続と CONNACK)、`TinyMQClientSn` の手順(応答まで、再送込み)、`TinyMQTcpListener` の書き込み(Client 次第)。`connectAsync()` を使うと CONNACK は待ちません。`TinyMQClientSn::onIdle()` で、待つ間もほかの処理を回せます
- **コールバックの中で**: `onMessage` などから publish してかまいません。`TinyMQClientSn` のコールバックからは、待つ手順(`connect()`、QoS 1 / 2 の publish、`subscribe()`)を呼ばないでください
- **タスク**: ブローカーとクライアントは、1 つのタスク(普通は `loop()`)からだけ使ってください。複数のタスクから呼ぶ場合は、呼ぶ側で排他します
- **lwIP**: `TinyMQLwipListener` のコールバックは受信を溜めるだけで、ブローカーは `update()` の中で lwIP のロックを取って動きます(ESP32: コアロック、arduino-pico: `LWIPMutex`、CH32V: 不要)。arduino-pico の FreeRTOS モードでは `TinyMQLwipListener` は使えません

## 5. QoS、セッション、キュー

### クライアント

- QoS 1 / 2 で送ったメッセージは、確認までプール(`TINYMQ_CLIENT_POOL_BYTES`)に保持し、同時に `TINYMQ_CLIENT_MAX_INFLIGHT` まで。一杯なら `publish()` は −1 を返します
- 永続セッション(`setCleanSession(false)`、5.0 は `setSessionExpiry()` も)でつなぎ直すと、確認のない PUBLISH と PUBREL を DUP 付きで送り直します。ブローカーがセッションを持っていなければ(Session Present が 0)、手元の分も捨てます
- 受信バッファ(`TINYMQ_MAX_PACKET`)より大きい PUBLISH は、確認だけ返して捨てます(`droppedMessages()`)。接続は切りません。5.0 では Maximum Packet Size を伝えるので、ブローカーが送ってきません

### ブローカー

- 届いたメッセージは、購読しているセッションごとのキュー(`TINYMQ_BROKER_MAX_QUEUED`)に入れ、送信中の QoS 1 / 2 は `TINYMQ_BROKER_MAX_INFLIGHT`(5.0 はクライアントの Receive Maximum も)まで。順番を守るため、上限に達したら後ろのメッセージも待ちます
- キューやプールが一杯のときはメッセージを捨てます(`droppedMessages()`)
- 3.1.1 のセッションには、重なる購読があっても最大の QoS で 1 回届けます。5.0 のセッションには購読ごとに届けます(購読識別子とオプションが購読ごとに違うため)
- 切断中のセッションには QoS 0 を貯めません。セッション表が一杯になると、切断中で最も古いセッションを捨てます。`setSessionExpiry()` で上限の時間を決められます
- QoS 2 の受信は、パケット ID だけを覚えて受信時に配る方式で、常に受け付けます。購読者への QoS 2 は `setMaxQos(1)` や `TINYMQ_ENABLE_QOS2_DELIVERY=0` で落とせます(SUBACK で許す QoS を下げる)
- 同じ Client ID の接続が来ると、前の接続を切ります(5.0 は DISCONNECT 0x8E)

## 6. MQTT 5.0

### クライアント

```cpp
mqtt.setProtocolVersion(tinymq::kMqtt5);
mqtt.setSessionExpiry(3600);                 // 切れても 1 時間セッションを保つ

uint8_t wp[8];
tinymq::PropWriter w(wp, sizeof(wp));
w.u32(tinymq::kWillDelayInterval, 30);       // 30 秒以内に戻れば will を出さない
mqtt.setWillProperties(w.result());

tinymq::SubOptions o;
o.qos = 1;
o.noLocal = true;                            // 自分の publish を受け取らない
mqtt.subscribe("room/#", o, 7);              // 購読識別子 7
```

- プロパティ付きの publish は `publish(topic, payload, len, qos, retain, props)`。受け取ったプロパティは `Message::props` を `PropReader` で読みます
- CONNACK でブローカーが伝えた Receive Maximum、Maximum QoS、Retain Available、Maximum Packet Size、Server Keep Alive に従います(超える publish は −1)。割り当てられた Client ID は `assignedClientId()`
- `disconnect(tinymq::kDisconnectWithWill)` で、切断しても will を出させられます
- こちらで見つけたプロトコル違反は、理由コード付きの DISCONNECT を送ってから閉じます。ブローカーからの DISCONNECT の理由は `onDisconnect` / `lastReason()`
- 拡張認証(AUTH)と再認証: §9

### ブローカー

- セッションとメッセージの有効期限、Will Delay(遅延中に戻れば取り消し、セッションが先に終われば出す)
- 購読識別子、No Local、Retain As Published、Retain Handling、共有購読(`$share/<group>/<filter>`。オンラインのメンバーへ順番に配り、retain は送らない)
- クライアントからのトピックエイリアス(`TINYMQ_BROKER_TOPIC_ALIAS_MAX`。ブローカーからは使わない)
- 理由コード: PUBACK の 0x10(購読者なし)/ 0x87(認可されない)/ 0x99(UTF-8 でないペイロード)、SUBACK の 0x8F / 0x87 / 0x97 / 0x9E、UNSUBACK の 0x11、プロトコル違反のときの DISCONNECT
- 拡張認証(AUTH)と再認証: §9

## 7. MQTT-SN

### ゲートウェイ(`TinyMQSnListener`)

- MQTT-SN のクライアント 1 つに、ブローカーの接続を 1 本割り当て、パケットを相互に読み替えます。QoS、retain、will、永続セッション、認可はブローカーのものがそのまま使われます(Client ID もブローカーと共通です)
- トピック ID の登録はクライアントごと。ワイルドカードの購読に配るときや、クライアントが知らない ID を「不正なトピック ID」で返したときは、ゲートウェイから REGISTER してから送ります
- 事前定義トピック(`addPredefinedTopic()`)と 2 文字の短いトピック名は、登録なしで使えます。QoS −1(接続なしの PUBLISH)はこの 2 つだけで受け付けます
- **スリープ**: クライアントが DISCONNECT に時間を付けると、その間のメッセージ(QoS 0 も)をブローカーのキューに預かり、起床の PINGREQ で配ってから PINGRESP を返します
- **UDP の取りこぼし**: 確認のない配送は Tretry(既定 10 秒)ごとに DUP 付きで送り直し、Nretry(既定 3)回を超えたらクライアントを切断扱いにして will を出します(`setRetry()`)
- **フォワーダー**: Forwarder Encapsulation で包まれたメッセージをほどきます。フォワーダーの後ろの Wireless Node Id ごとに別のクライアントで、返事はそのノード宛てに包んで返します(`TINYMQ_SN_MAX_NODE_ID`、既定 8 バイト)
- `setSourceFilter()` で、コールバックが断った送信元のデータグラムを捨てます。アドレス、ポート、フォワーダーの後ろならノード ID が見えます:

```cpp
bool onSource(const tinymq::Endpoint &from, void *)
{
    return from.ip[0] == 192 && from.ip[1] == 168 && from.ip[2] == 1;  // 192.168.1.x だけ
}

sn.setSourceFilter(onSource);
```

- 送信元アドレスで認証はしません(フィルターは振り分けるだけです)。**閉じたネットワークか、TinyWG のトンネルの中で**使ってください

### クライアント(`TinyMQClientSn`)

| 段階(`TINYMQ_SN_CLIENT_LEVEL`) | 使えるもの | CH32V307 の増分 |
|---|---|---|
| 0 | QoS −1 の送信だけ(`publishPredefined()`、`publishShort()`) | Flash +1.0 KB / RAM +0.4 KB |
| 1 | 接続、登録、QoS 0 / 1 の送信、購読、keepalive、再送 | +5.1 KB / +1.2 KB |
| 2(既定) | 1 に加えて QoS 2、will、スリープ、ゲートウェイの探索 | +6.0 KB / +1.2 KB |

- 手順はゲートウェイの応答を待ち、Tretry ごとに送り直します。送信中のメッセージをコピーして持たないので、RAM はバッファとトピックの表だけです
- トピック ID の表は、つなぎ直しても消しません(スリープから戻ったゲートウェイは登録を覚えているため)。古い対応は、ゲートウェイの REGISTER や「不正なトピック ID」の返事で直ります
- スリープ: `sleep(秒)` でゲートウェイに預けさせ、ときどき `checkMessages()` で受け取ります。`connect()` でスリープを終えます

## 8. ブリッジ

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

- 対応表は mosquitto の `topic <pattern> <方向> <QoS> <ローカルのプレフィックス> <上流のプレフィックス>` と同じ考え方です
- **ループ**: ブリッジは、自分がブローカーに入れたメッセージを受け取りません。上流側は、5.0 なら No Local で購読するので、`kBoth` でも戻ってきません。3.1.1 で `kBoth` を使うなら、両側で別のプレフィックスにしてください
- retain フラグは保ったまま転送します。つながるたびに、上流の購読とローカルの retain の送信をやり直します(上流が retain を失っていても戻る)
- 上流が落ちている間(または上流の送信中が一杯の間)に外へ出す QoS 1 / 2 は、ブリッジが預かり(`queued()`、`TINYMQ_BRIDGE_QUEUE_BYTES` / `TINYMQ_BRIDGE_MAX_QUEUED`)、つながったら古い順に送ります。QoS 0 と、預かりきれないものは捨てて数えます(`dropped()`)。預かったものは再起動で消えます
- 上流への接続は `connectAsync()` で始めるので、CONNACK を待つ間もブローカーは止まりません(TCP の接続そのものは Client の `connect()` の分だけ待ちます)

## 9. 認証と認可

```cpp
uint8_t onAuth(const tinymq::ConnectInfo &info, void *)
{
    if (info.hasUsername && info.username.equals("sensor") && info.hasPassword && info.password.equals("secret"))
        return tinymq::kSuccess;
    return tinymq::kBadUserNameOrPassword;    // 3.1.1 では戻りコード 4 になる
}

bool onAcl(tinymq::Bytes clientId, tinymq::Bytes user, tinymq::Bytes topic, bool subscribe, void *)
{
    if (subscribe)
        return true;
    return topic.len >= 8 && memcmp(topic.data, "sensors/", 8) == 0;  // publish は sensors/ の下だけ
}

broker.onAuthenticate(onAuth);
broker.onAuthorize(onAcl);
```

- 認可されない publish は、3.1.1 では確認だけ返して捨て、5.0 では PUBACK 0x87 を返します。認可されない購読は SUBACK で失敗(0x80 / 0x87)
- `$SYS` で始まるトピックへの publish は、クライアントからは受け付けません(確認して捨てる)
- MQTT-SN のクライアントにも同じ認可が効きます(Client ID で判断できます。ユーザ名はありません)

### 拡張認証(5.0 の AUTH)

CONNECT に Authentication Method を入れた 5.0 のクライアントは `onEnhancedAuth()` を通ります。チャレンジ・レスポンス(SCRAM など)やトークンに使えます。ハンドラは 1 往復ずつ呼ばれ、`kContinueAuthentication`(返事を AUTH でクライアントに送る)、`kSuccess`、拒否の理由コードのどれかを返します。接続中のクライアントからの再認証(AUTH 0x19)も同じハンドラで、失敗すると接続を切ります。

```cpp
uint8_t onEnhancedAuth(tinymq::AuthStep &st, void *)
{
    if (!st.method.equals("TOKEN"))
        return tinymq::kBadAuthenticationMethod;
    if (!st.data.equals("my-token"))
        return tinymq::kNotAuthorized;
    return tinymq::kSuccess;
}

broker.onEnhancedAuth(onEnhancedAuth);
```

- `st.step` は 1 回のやり取りの中の何往復目か(0 から)、`st.conn` は接続の番号です。往復の間で状態を持つ方式に使います。返事(`st.reply`、`TINYMQ_AUTH_DATA` バイトまで、既定 128)は AUTH で、最後の往復なら CONNACK で送ります
- 拡張認証が通ったあとも `onAuthenticate()` は呼ばれます
- `onEnhancedAuth()` がなければ、Authentication Method を含む CONNECT は 0x8C で断ります
- クライアントでは、方式と最初のデータを `setConnectProperties()` で CONNECT に入れ、`onAuth()` で AUTH に答え、最後の CONNACK のデータも受け取ります。`reauthenticate(data)` で再認証を始めます

## 10. 永続化

`saveState()` は、retain(期限の残り時間も)と、有効期限のあるセッション(購読、未配送の QoS 1 / 2)を書き出します。`loadState()` は、`begin()` のあと、受け口を登録する前に呼びます。

- 送信中だった QoS 1 / 2 は、読み戻したあと最初から送り直します。QoS 1 は重複がありえます。QoS 2 の受信中の状態と、遅延中の will は保存しません
- 保存のきっかけには `onRetainedChange()` が使えます。セッションの変化(購読、キュー)は頻繁なので、時間を決めて保存するのが現実的です
- Flash に保存するときは、新しいファイルに書いてから入れ替えると、保存中のリセットでも前の状態が残ります([`Esp32Persist`](../examples/Esp32Persist/README.ja.md))

## 11. 診断

| 見るもの | 分かること |
|---|---|
| `TinyMQClient::lastReason()` / `onDisconnect` | 接続が終わった理由(0x80 は通信が切れた、0x8D は keepalive 切れ、0x8E は乗っ取り) |
| `TinyMQClient::droppedMessages()` | 受信バッファより大きくて捨てた数 |
| `TinyMQBroker::droppedMessages()` | キュー、プール、retain の表が一杯で捨てた数 |
| `TinyMQBroker::poolUsed()` / `poolCapacity()` | メッセージプールの使用量 |
| `clients()` / `sessions()` / `subscriptions()` / `retained()` | 表の埋まり具合 |
| `$SYS/broker/...` | `broker.setSysInterval(10)` で、稼働時間、接続数、セッション数、購読数、retain の数、受信・送信・破棄したメッセージ数、プールの使用量を 10 秒ごとに retain で出す(mosquitto と同じ名前)。retain の枠を 11 使う |
| `TinyMQBridge::dropped()` | 上流が落ちていて捨てた数 |
| `TinyMQClientSn::lastReturnCode()` | MQTT-SN の戻りコード |

## 12. 制限

- MQTT-SN のゲートウェイとクライアントは IPv4 のみ
- ブローカー側の TLS、WebSocket、MQTT-SN 2.0 は対象外
- ブリッジが預かるメッセージは RAM の中だけ(再起動で消える)
- ブローカーはトピックエイリアスを自分からは使いません(受けるだけ)
