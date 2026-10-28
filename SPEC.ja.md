# TinyMQ 仕様書（ドラフト）

[English](SPEC.md)

> ステータス: 方針決定(2026-10-28)。実装は未着手。まだ決めていない値は `TBD` と表記する。

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
| 拡張認証(AUTH) | — | `TBD` | コールバックの形だけ用意するか検討 |
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
| Forwarder Encapsulation | `TBD` |

セッションの識別はクライアントのアドレスとポートで行う(MQTT-SN 1.2 と同じ)。スリープからの復帰で送信元が変わった場合は Client ID で引き当てる。

**セキュリティ**: MQTT-SN 1.2 には暗号も認証もない。閉域網か、TinyWG のトンネル内で使う前提とし、ドキュメントで明記する。受け口ごとに送信元アドレスの許可リストを設定できる。

### 4.5 MQTT-SN 1.2 — クライアント(`TinyMQClientSn`)

プアなチップ向けなので、**機能をビルド時に段階的に選べる**ようにする。

| 段階 | 含むもの |
|---|---|
| 最小 | QoS −1 の PUBLISH(事前定義 ID・短いトピック名)のみ。接続の状態を持たない |
| 標準(既定) | CONNECT、REGISTER、PUBLISH QoS 0 / 1、SUBSCRIBE、PINGREQ、再送 |
| 全部 | 上記 + QoS 2、will、スリープ、ゲートウェイ探索 |

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

ビルド時のマクロ(`-D` で上書き、`src/tinymq/mq_config.h`)と、実行時の設定(`begin()` 前後のセッター)の 2 段にする。ビルド時に外した機能は、実行時に有効にできない。既定値は実測後に決める。

| マクロ(案) | 内容 |
|---|---|
| `TINYMQ_BROKER_MAX_CLIENTS` | 同時接続数(TCP + MQTT-SN) |
| `TINYMQ_BROKER_MAX_SESSIONS` | 保持するセッション数(切断中の永続セッションを含む) |
| `TINYMQ_BROKER_MAX_SUBSCRIPTIONS` | 購読の総数 |
| `TINYMQ_MAX_TOPIC_LEN` | トピック名の最大長 |
| `TINYMQ_MAX_PACKET` | 受け付けるパケットの最大長 |
| `TINYMQ_BROKER_POOL_BYTES` | メッセージプールの大きさ(0 なら `begin()` で確保) |
| `TINYMQ_BROKER_MAX_RETAINED` | retain の保持数 |
| `TINYMQ_BROKER_MAX_INFLIGHT` | クライアントごとの送信中の上限 |
| `TINYMQ_BROKER_MAX_QUEUED` | クライアントごとの切断中・スリープ中のキュー |
| `TINYMQ_ENABLE_MQTT5` | 5.0 を含める |
| `TINYMQ_ENABLE_QOS2_DELIVERY` | QoS 2 の配送を含める(§4.3) |
| `TINYMQ_ENABLE_PERSISTENT_SESSION` | 永続セッション |
| `TINYMQ_ENABLE_SHARED_SUB` / `TINYMQ_ENABLE_TOPIC_ALIAS` / `TINYMQ_ENABLE_SYS` | 5.0 の個別機能、`$SYS` |
| `TINYMQ_ENABLE_SN` | MQTT-SN ゲートウェイ |
| `TINYMQ_SN_CLIENT_LEVEL` | MQTT-SN クライアントの段階(§4.5) |
| `TINYMQ_ENABLE_BRIDGE` | ブリッジ |

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
