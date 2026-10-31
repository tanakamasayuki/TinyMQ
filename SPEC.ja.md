# TinyMQ 仕様書（ドラフト）

[English](SPEC.md)

> ステータス: Phase 0〜4、6〜8 を実装済み(2026-10-28)。ブリッジ、lwIP の受け口、永続化も含む。MQTT 3.1.1 / 3.1 / 5.0 のクライアントとブローカー(拡張認証を含む)、MQTT-SN 1.2 のゲートウェイとクライアント。ESP32 / ESP32-S3 の実機で試験済み(Wi-Fi)。まだ決めていない値は `TBD` と表記する。

## 1. 目的と範囲

**TinyMQ** は Arduino 向けの、コアに依存しない MQTT ライブラリ。**クライアントとブローカーの両方**を含み、使う側だけを選んで使う(使わない側はリンクされない)。

- 基本は TCP の通常の MQTT(3.1.1 / 5.0)
- ブローカーは UDP の **MQTT-SN 1.2** も受け付け、TCP のクライアントと同じトピック空間で中継する。TCP/MQTT を載せるのが重いチップは UDP で参加する
- UDP 側のクライアント(MQTT-SN)も TinyMQ に含める
- ブローカーは ESP32 / Pico W 程度のスペックを想定し、**なるべく仕様の全体を実装する**。そのうえで、ビルド時・実行時の設定で機能を落として軽くできるようにする

### 1.1 想定ユースケース

| # | 構成 | TinyMQ の役割 |
|---|---|---|
| UC1 | ESP32 等 → クラウドや LAN の MQTT ブローカー(mosquitto 等) | `TinyMQClient`(TCP) |
| UC2 | ESP32 / Pico W を LAN のブローカーにし、PC・スマホ・他のマイコンが TCP で、小型 MCU(CH32V 等)が UDP(MQTT-SN)で参加 | `TinyMQBroker` + TCP / MQTT-SN の受け口 / 端末: `TinyMQClient` または `TinyMQClientSn` |
| UC3 | UC2 のブローカーを TinyWG のサーバーと同居させ、端末はトンネル越しに参加(UDP 側の暗号化と認証を WireGuard に任せる) | 同上 + TinyWG |
| UC4 | UC2 のブローカーが、一部のトピックを上流(クラウド)のブローカーへ転送する | `TinyMQBroker` + ブリッジ |
| UC5 | スケッチ自身がブローカー内で publish / subscribe する(ネットワークを経由しない) | ブローカーのローカルクライアント |

### 1.2 対象プラットフォーム

TinyWG と同じコアを対象にする。

| コア | クライアント | ブローカー | 備考 |
|---|---|---|---|
| ESP32 / ESP32-S3 (arduino-esp32 3.3) | ○ | ○(主対象) | |
| RP2040 / RP2350 (arduino-pico 6.3、Pico W / Pico 2 W) | ○ | ○(主対象) | |
| CH32V (ArduinoCore-CH32RV 0.4、Network ライブラリ) | ○(主に MQTT-SN) | 設定で軽くした構成 `TBD` | |
| host (lang-ship:host) | ○ | ○ | `WiFiClient` / `WiFiServer` / `WiFiUDP` が実ソケットで動くので、mosquitto・paho-mqtt と相互接続試験をする |

クライアントは、`Client&` または `UDP&` を持つコアなら原則どこでも動く(W5500 等の lwIP を持たない Ethernet を含む)。

## 2. 対象外

- MQTT over WebSocket
- ブローカー側の TLS(クライアント側は `Client&` に `WiFiClientSecure` 等を渡せば使える)
- MQTT-SN 2.0(OASIS で策定中。1.2 が安定してから検討)
- クラスタリング

## 3. アーキテクチャ

```
App(スケッチ)
 ├ TinyMQClient        MQTT 3.1.1 / 5.0 クライアント(Arduino Client& の上)
 ├ TinyMQClientSn      MQTT-SN 1.2 クライアント(Arduino UDP& の上)
 └ TinyMQBroker        ブローカー本体 + ローカルクライアント + ブリッジ
        │ 受け口(Listener)を複数登録できる
        ├ TinyMQTcpListener<ServerT>   Arduino の WiFiServer / NetworkServer / EthernetServer 等
        ├ TinyMQSnListener             Arduino UDP&(MQTT-SN ゲートウェイ)
        └ TinyMQLwipListener           lwIP の tcp_pcb 直叩き(後のフェーズ)
[tinymq::core]   コーデック(3.1.1 / 5.0 / MQTT-SN)、セッション、購読表、トピック照合、
                 retain、メッセージプール      ← Arduino にも lwIP にも依存しない
[Port]           時刻(millis 相当)、排他(lwIP 版のみ)
```

### 3.1 層の責務

- **core**: 純 C++。入力は「接続ごとのバイト列(TCP)」または「データグラム(UDP)」、出力も同じ。host で単体テストする
  - 内部表現は **MQTT 5.0 を基準**にする(理由コード、プロパティ、セッション有効期限など)。3.1.1 は 5.0 の部分集合として扱い、エンコード時に落とす。MQTT-SN のセッションも同じセッション表に入れる
- **ブローカーと受け口**: core は「接続」を `送信可能量の問い合わせ / 送信 / 切断` の抽象だけで扱う。受け口は複数を同時に登録でき、TCP と MQTT-SN のクライアントは区別なく同じトピック空間で中継される
- **外部の輸送を使える形にする**: TinyWG と同じく、クライアントは `Client&` / `UDP&` を受け取り、ブローカーの受け口もユーザの `Server` / `UDP` を受け取る。lwIP 直叩きの受け口は最適化として後から足す

### 3.2 実行モデル

- Arduino API の受け口とクライアントは**ポーリング型**: `loop()` から `update()` を呼ぶ。`update()` はブロックしない(書けない分は次回に回す)
- `Server` 基底クラスには `accept()` がなく、コアごとに型が違うため、TCP の受け口はテンプレートにして `accept()` / `available()` の戻り値の型から Client の型を決める
- lwIP の受け口(後のフェーズ)はコールバックで動く。排他は TinyWG の §3.2 と同じ方法をとる
- core はスレッド非前提。複数タスクから呼ぶ場合の排他は `TBD`(ESP32 でブローカーを別タスクに置く形を想定)

## 4. 機能要件

### 4.1 MQTT(TCP)— ブローカー

目標は MQTT 3.1.1 / 5.0 の仕様の全体。`[軽量化]` は設定で外せるもの(§5)。

| 機能 | 3.1.1 | 5.0 | 備考 |
|---|---|---|---|
| QoS 0 / 1 / 2 | ○ | ○ | QoS 2 の配送は `[軽量化]`(§4.3) |
| retain | ○ | ○ | 保持数・総バイト数に上限。`[軽量化]` |
| will | ○ | ○ | 5.0 の Will Delay Interval を含む |
| keepalive(1.5 倍で切断) | ○ | ○ | 5.0 の Server Keep Alive で上書き可 |
| clean session / clean start + セッション有効期限 | ○ | ○ | |
| 永続セッション(切断中のメッセージ保持) | ○ | ○ | クライアントごとの件数・全体のバイト数に上限。`[軽量化]` |
| セッションの乗っ取り(同じ Client ID) | ○ | ○ | |
| ワイルドカード `+` / `#`、`$` で始まるトピックの扱い | ○ | ○ | |
| 認証(ユーザ名 / パスワード) | ○ | ○ | コールバックで判定 |
| 認可(トピックごとの publish / subscribe 可否) | ○ | ○ | コールバックで判定 |
| Client ID の自動割り当て | ○ | ○ | |
| 送信中(inflight)の上限、受信の上限 | ○ | ○ | 5.0 は Receive Maximum |
| プロパティ全般の受け渡し(ユーザープロパティ、Content Type、Response Topic、Correlation Data 等) | — | ○ | |
| トピックエイリアス | — | ○ | `[軽量化]` |
| 共有購読 `$share/` | — | ○ | `[軽量化]` |
| 購読識別子、No Local、Retain As Published、Retain Handling | — | ○ | |
| メッセージ有効期限 | — | ○ | |
| Maximum Packet Size、Maximum QoS、Retain Available 等の通知 | — | ○ | 設定で落とした機能は CONNACK で正しく通知する |
| サーバーからの DISCONNECT(理由コード付き) | — | ○ | |
| 拡張認証(AUTH) | yes | yes | 1 往復ごとのコールバック(`onEnhancedAuth()` / `onAuth()`)。再認証も |
| `$SYS` トピック | ○ | ○ | `[軽量化]`。内容は mosquitto に倣う `TBD` |
| MQTT 3.1(プロトコル名 `MQIsdp`) | ○ | — | 古いクライアント向け。安価なので受け付ける `TBD` |

### 4.2 MQTT(TCP)— クライアント

- MQTT 3.1.1 / 5.0、QoS 0 / 1 / 2、retain、will、永続セッション(再接続時の再送)、keepalive、自動再接続(任意)
- 5.0: プロパティ、トピックエイリアス(送受信)、理由コードの取得
- 受信はコールバック(トピック、ペイロード、QoS、retain、5.0 のプロパティ)
- `Client&` を受け取るので、TLS は `WiFiClientSecure` 等を渡せば使える
- ブローカーのブリッジもこのクライアントを使う

### 4.3 QoS 2 を落とす

QoS 2 の重さは向きで違う。

- **受信(クライアント → ブローカー)は安い**: 仕様の「パケット ID だけを覚えて受信時に転送する」方式(3.1.1 §4.3.3 の Method B 相当)をとる。本文は保持せず、クライアントごとの ID の集合だけを持つ。**常に受け付ける**
- **配送(ブローカー → 購読者)は高い**: PUBREC までメッセージを保持し、その後 PUBREL の状態を持つ

「QoS 2 を落とす」は、配送の最大 QoS を 1 にすることを指す。3.1.1 では SUBACK で許可する QoS を下げる(配送は min(発行 QoS, 許可 QoS))、5.0 では加えて CONNACK の Maximum QoS で通知する。ビルド時(コードごと外す)と実行時の両方で選べる。

### 4.4 MQTT-SN 1.2 — ブローカー側(ゲートウェイ)

ブローカーに組み込んだ集約型ゲートウェイとして実装する。MQTT-SN のクライアントもブローカーのセッション表に直接入る(TCP の MQTT に変換して別のブローカーへ流す形ではない)。

| 機能 | 備考 |
|---|---|
| CONNECT / WILLTOPICREQ / WILLTOPIC / WILLMSGREQ / WILLMSG / CONNACK | |
| REGISTER / REGACK(クライアントから / ゲートウェイから) | トピック ID はクライアントごとに割り当てる |
| 事前定義トピック ID | API でブローカーに登録する。全クライアント共通 |
| 短いトピック名(2 文字) | |
| PUBLISH QoS −1 / 0 / 1 / 2 | QoS −1 は接続なしで受け付ける(事前定義 ID・短いトピック名のみ)。`[軽量化]` で無効化可 |
| SUBSCRIBE / UNSUBSCRIBE(トピック名・事前定義 ID・短いトピック名、ワイルドカード) | ワイルドカード購読の配送時は、ゲートウェイから REGISTER してから PUBLISH |
| PINGREQ / PINGRESP、keepalive | |
| スリープするクライアント(DISCONNECT の duration、asleep / awake、起床時の配送) | 保持するメッセージ数に上限。`[軽量化]` |
| WILLTOPICUPD / WILLMSGUPD | |
| ADVERTISE / SEARCHGW / GWINFO | ブロードキャストの可否はコア依存。`[軽量化]` |
| 再送(Tretry / Nretry) | |
| Forwarder Encapsulation | フォワーダーの後ろの Wireless Node Id ごとに別のクライアント。返事はそのノード宛てに包んで返す(ID は `TINYMQ_SN_MAX_NODE_ID`、8 バイトまで) |

セッションの識別はクライアントのアドレスとポートで行う(MQTT-SN 1.2 と同じ)。スリープからの復帰で送信元が変わった場合は Client ID で引き当てる。

**セキュリティ**: MQTT-SN 1.2 には暗号も認証もない。閉域網か、TinyWG のトンネル内で使う前提とし、ドキュメントで明記する。受け口ごとに、送信元をコールバック(`setSourceFilter()`: アドレス、ポート、ノード ID)で選べる。

### 4.5 MQTT-SN 1.2 — クライアント(`TinyMQClientSn`)

プアなチップ向けなので、**機能をビルド時に段階的に選べる**ようにする(`TINYMQ_SN_CLIENT_LEVEL`)。

| 段階 | 含むもの |
|---|---|
| 0 | QoS −1 の PUBLISH(事前定義 ID・短いトピック名)のみ。接続の状態を持たない |
| 1 | CONNECT、REGISTER、PUBLISH QoS 0 / 1、SUBSCRIBE、PINGREQ、再送 |
| 2(既定) | 上記 + QoS 2、will、スリープ、ゲートウェイ探索 |

既定を 2 にしたのは、Arduino IDE からは `-D` を渡しにくいことと、スケッチが呼ばない関数はどの段階でもリンカが落とすため。段階を下げると `update()` が引き込むコードとバッファが減る。

- 手順(接続、登録、購読、QoS 1 / 2 の送信)は応答が来るまで待つ(ブロッキング)。応答がなければ Tretry ごとに再送し(PUBLISH は DUP 付き)、Nretry 回で諦める。送信中のメッセージをコピーして持たないので、RAM はバッファとトピックの表だけで済む。待っている間に届いたメッセージはコールバックで渡す
- 待っている間に呼ぶフック(`onIdle()`)で、ほかの処理(同じボードのブローカーなど)を止めずに済む
- トピック ID の表は接続し直しても消さない。スリープから戻ったゲートウェイは登録を残しているため。古い対応は、ゲートウェイの REGISTER で上書きされるか、PUBACK の「不正なトピック ID」を受けて登録し直す

CH32V307(Ethernet で 1 秒ごとに UDP を送るスケッチとの差、`TINYMQ_SN_CLIENT_BUFFER` 256、2026-10-28):

| 段階 | Flash | RAM(静的) |
|---|---|---|
| 0 | +1.0 KB | +0.4 KB |
| 1 | +5.1 KB | +1.2 KB |
| 2 | +6.0 KB | +1.2 KB |

### 4.6 ローカルクライアントとブリッジ

- **ローカルクライアント**: スケッチがブローカーの中で `publish()` / `subscribe()` する。ネットワークを経由せず、他のクライアントと同じ購読・retain・QoS の規則で配送される
- **ブリッジ**: ブローカーが `TinyMQClient` で上流のブローカーへ接続し、トピックの対応表(方向 in / out / both、プレフィックスの付け替え、QoS)に従って転送する。mosquitto の bridge 設定に相当。ループ防止(5.0 なら No Local、3.1.1 ならプレフィックス)を入れる
- 複数の上流 `TBD`

### 4.7 永続化

- retain と永続セッションを Flash 等に保存するためのフック(保存・読み出しのコールバック)を用意する。具体的な保存先(LittleFS 等)はサンプルで示す
- 既定はメモリのみ(再起動で消える)

## 5. 非機能要件

| 項目 | 方針 |
|---|---|
| メモリ確保 | 実行中の動的確保なし。接続表・セッション表・購読表は静的確保。可変長のメッセージ(retain、キュー、inflight)は **メッセージプール**(バイト領域を参照カウント付きで共用。複数の購読者への配送で本文をコピーしない)に置く。プールは静的配列か、`begin()` 時に 1 回だけ確保(ESP32 の PSRAM を含む)かを選べる |
| 実行モデル | ポーリング型(`update()`)。lwIP 版はコールバック型(§3.2) |
| Flash / RAM | 実測して記録する。目標値は実測後に決める |
| スループット・遅延 | 実測して記録する(ブローカーでの 1 対多の配送、QoS ごと) |
| 互換性 | mosquitto / paho-mqtt(Python)/ Eclipse の相互接続試験スイートで確認する |

### 5.1 設定

ビルド時のマクロ(`-D` で上書き、`src/tinymq/mq_config.h`)と、実行時の設定(`begin()` 前後のセッター)の 2 段にする。全一覧と既定値は[上級者ガイド](docs/guide-advanced.ja.md) §2。

- 表の大きさ: 接続数、セッション数、購読数、retain の数、セッションごとのキューと送信中の数、受信中の QoS 2、トピックエイリアス、MQTT-SN のクライアント数と登録数、ブリッジの対応表
- バッファ: `TINYMQ_MAX_PACKET`、`TINYMQ_BROKER_RX_BUFFER`、プール(`TINYMQ_BROKER_POOL_BYTES`、0 なら `begin()` で 1 回確保)
- プリセット: `TINYMQ_PRESET_SMALL`(接続数と最大のパケットはそのままで、表とプールを小さくする。値は上級者ガイド §2)
- 機能: `TINYMQ_ENABLE_MQTT5`、`TINYMQ_ENABLE_SHARED_SUB`、`TINYMQ_ENABLE_QOS2_DELIVERY`、`TINYMQ_SN_CLIENT_LEVEL`

当初案にあった `TINYMQ_ENABLE_SN` / `TINYMQ_ENABLE_BRIDGE` / `TINYMQ_ENABLE_PERSISTENT_SESSION` などは設けていない。MQTT-SN、ブリッジ、永続化は、使わなければリンカが落とす。`$SYS` は未実装。

### 5.2 ブローカーの性能(2026-10-28、`tests/bench_broker`、ESP32-S3、家庭用の Wi-Fi、PC の paho-mqtt から)

| | `TinyMQLwipListener` | `TinyMQTcpListener<WiFiServer>` |
|---|---|---|
| 往復の遅延 QoS 0(中央値 / p95) | 7.9 / 17.7 ms | 249 / 268 ms(遅延 ACK、下記) |
| 往復の遅延 QoS 1(中央値 / p95) | 7.7 / 17.4 ms | 10.7 / 20.2 ms |
| 1 対 1 の連続送信 QoS 0(100 バイト) | 2967 msg/s(届いた割合 71%) | 916 msg/s(100%) |
| 1 対 1 の連続送信 QoS 1 | 125 msg/s | 88 msg/s |
| 1 対 4 の配送 QoS 0 | 6913 件/s(76%) | 1350 件/s(100%) |

- QoS 1 の連続送信は、paho が同時に 20 件までしか送らないので、Wi-Fi の往復で決まる
- QoS 0 の取りこぼしは、購読者のキュー(`TINYMQ_BROKER_MAX_QUEUED`、既定 32)が溢れた分で、`droppedMessages()` に数える。`TinyMQTcpListener` は書き込みで待つので、送り手の TCP が抑えられて取りこぼさないが、全体が遅くなる
- **遅延 ACK**: Nagle を止めていないクライアント(paho-mqtt など)の QoS 0 は、ブローカー側の遅延 ACK(lwIP で最大 250 ms)を待たされる。`TinyMQLwipListener` は受け取るたびにすぐ ACK を返すので起きない
- 計測の途中で、`TinyMQLwipListener` の 2 つの問題を直した: 送信キューのセグメント数(`TCP_SND_QUEUELEN`)を見ずに書いて接続が切れていたこと、書き込みのたびに `tcp_output()` を呼んで小さなパケットが別々のセグメントになっていたこと(後者で 1 対 1 は 244 → 2967 msg/s)

ESP32(D0WDQ6、1-2、同じ Wi-Fi、2026-10-31):

| | `TinyMQLwipListener` | `TinyMQTcpListener<WiFiServer>` |
|---|---|---|
| 往復の遅延 QoS 0(中央値 / p95) | 3.48 / 10.3 ms | 243 / 451 ms(遅延 ACK) |
| 往復の遅延 QoS 1(中央値 / p95) | 4.16 / 13.7 ms | 5.08 / 33.9 ms |
| 1 対 1 の連続送信 QoS 0(100 バイト) | 3787 msg/s(届いた割合 82.5%) | 1241 msg/s(100%) |
| 1 対 1 の連続送信 QoS 1 | 175 msg/s | 174 msg/s |
| 1 対 4 の配送 QoS 0 | 7278 件/s(61%) | 1134 件/s(100%) |

ESP32-P4(2-3、有線 LAN 100 Mbps、2026-10-31):

| | `TinyMQLwipListener` | `TinyMQTcpListener<WiFiServer>` |
|---|---|---|
| 往復の遅延 QoS 0(中央値 / p95) | 1.37 / 2.24 ms | 207 / 311 ms(遅延 ACK) |
| 往復の遅延 QoS 1(中央値 / p95) | 0.83 / 1.69 ms | 1.60 / 2.92 ms |
| 1 対 1 の連続送信 QoS 0(100 バイト) | 4379 msg/s(届いた割合 35%) | 2338 msg/s(100%) |
| 1 対 1 の連続送信 QoS 1 | 555 msg/s | 546 msg/s |
| 1 対 4 の配送 QoS 0 | 2442 件/s(100%) | 2286 件/s(100%) |

- P4 用の arduino-esp32 は lwIP のコアロック(`CONFIG_LWIP_TCPIP_CORE_LOCKING`)なしでビルドされている。`TinyMQLwipListener` はそのとき lwIP の呼び出しを `tcpip_api_call()` で lwIP のスレッドに渡す(1 回ごとにスレッドの切り替えが入る)。これに気づく前は、ロックなしで lwIP を触って接続が壊れていた
- 1 対 1 の QoS 0 は、PC が送るほうが速く、購読者のキューが溢れた分を落としている(S3 と同じ理由)

接続ごとの RAM(2026-10-31、`bench_broker`: 購読付きのクライアントを 6 本つなぎ、前後の空きヒープを比べた):

| | ESP32-S3(Wi-Fi) | ESP32-P4(有線 LAN) |
|---|---|---|
| `TinyMQBroker` 自体(静的、8 接続、既定の設定) | 37996 バイト | 37996 バイト |
| 接続ごとのヒープ、`TinyMQTcpListener<WiFiServer>` | 1271 バイト | 2599 バイト |
| 接続ごとのヒープ、`TinyMQLwipListener` | 267 バイト | 261 バイト |

- `TINYMQ_BROKER_MAX_CLIENTS` の 1 接続ごとに、ブローカーの静的な大きさが約 1.1 KB 増える(受信バッファ `TINYMQ_BROKER_RX_BUFFER` の 1024 バイトと状態)。`TINYMQ_PRESET_SMALL` では、接続数によらず約 16 KB 小さい(表とプール)
- ヒープは lwIP の PCB とコアのクライアントのオブジェクト。`WiFiClient` は、lwIP の受け口より接続ごとに多くを持つ

## 6. テスト方針

TinyWG と同じ構成(pytest-embedded + Arduino CLI、`TEST done N/M` プロトコル、`sketch.yaml` のプロファイルでコアを固定)を踏襲する。

- **host 単体**: コーデックのエンコード / デコード(仕様書の例と不正なパケット)、トピック照合、購読表、メッセージプール
- **host 相互接続**(lang-ship:host の実ソケット)
  - `TinyMQClient` ↔ mosquitto
  - paho-mqtt(Python)↔ `TinyMQBroker`。QoS ごとの配送、retain、will、永続セッション、5.0 の各機能
  - Eclipse paho.mqtt.testing の相互接続試験スイートを `TinyMQBroker` に対して実行する(テストの道具としてのみ使い、配布物には含めない)
  - MQTT-SN: Python で書くテスト用の MQTT-SN ピア(TinyWG の `wg_peer.py` に相当)↔ `TinyMQBroker` / `TinyMQClientSn`。可能なら既存の MQTT-SN 実装とも突き合わせる
  - TCP と MQTT-SN の混在(MQTT-SN で publish → paho で受信、その逆)
- **実機**: ESP32 / ESP32-S3 / Pico W をブローカー、CH32V307 を MQTT-SN クライアントにした組み合わせ。TinyWG のトンネル越しの構成(UC3)
- **計測**: フットプリント、同時接続数ごとの RAM、配送のスループットと遅延

## 7. 進め方

作りやすい順に進め、最終的に全部を実装する。

1. **Phase 0 — 雛形**: `library.properties`、`src/` の構成、`tests/`(pytest、`sketch.yaml`)、`tools/bump_version.py`、リリースワークフローを TinyWG から持ってくる
2. **Phase 1 — core**: 3.1.1 のコーデック(内部表現は 5.0 基準)、トピック照合、購読表、メッセージプール。host の単体テスト
3. **Phase 2 — `TinyMQClient`(3.1.1)**: QoS 0 / 1 / 2、retain、will、永続セッション。mosquitto と相互接続
4. **Phase 3 — `TinyMQBroker`(3.1.1)+ `TinyMQTcpListener`**: 3.1.1 の全体、ローカルクライアント。paho-mqtt と相互接続試験スイート
5. **Phase 4 — MQTT-SN**: コーデック、`TinyMQSnListener`、`TinyMQClientSn`。Python の MQTT-SN ピアと、TCP との混在
6. **Phase 5 — 実機**: ESP32 / ESP32-S3 / Pico W / CH32V307 で動作確認と計測。TinyWG との組み合わせ
7. **Phase 6 — MQTT 5.0**: コーデックのプロパティ、クライアント、ブローカーの 5.0 機能
8. **Phase 7 — ブリッジ**
9. **Phase 8 — lwIP の受け口と永続化のフック**
10. 実測結果をもとに既定値と、設定の段階(軽量 / 標準 / 全部)のプリセットを決める

## 8. ライセンスと商標

- TinyMQ 本体は **MIT**。第三者のコードは同梱しない
- プロトコル層は公開仕様(OASIS MQTT 3.1.1、MQTT 5.0、MQTT-SN 1.2)から実装する。既存の実装(Eclipse Paho、mosquitto 等)のコードは流用しない
- mosquitto、paho-mqtt、paho.mqtt.testing 等は**テストの相手として使うだけ**で、配布物には含めない
- "MQTT" はプロトコル名として記述的に使用する
