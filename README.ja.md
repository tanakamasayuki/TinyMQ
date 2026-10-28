# TinyMQ

[English](README.md)

Arduino 向けの、コアに依存しない MQTT ライブラリです。**クライアントとブローカーの両方**を含み、使う側だけがリンクされます。

- `TinyMQClient`: 任意の Arduino `Client` の上で動く MQTT 3.1.1 / 5.0 クライアント。`WiFiClientSecure` を渡せば TLS も使えます
- `TinyMQBroker`: MQTT 3.1.1 / 3.1 / 5.0 のブローカー。QoS 0 / 1 / 2、retain、will、永続セッション、共有購読、認証・認可のコールバック、スケッチ自身の publish / subscribe
- `TinyMQSnListener`: ブローカーに組み込む MQTT-SN 1.2 のゲートウェイ。TCP が重い小さなチップは UDP で参加し、TCP のクライアントと同じトピック空間でやり取りします
- `TinyMQClientSn`: 任意の Arduino `UDP` の上で動く MQTT-SN 1.2 クライアント。QoS −1 の送信だけなら Flash 1 KB ほど
- `TinyMQBridge`: ブローカーから上流のブローカーへのブリッジ(mosquitto の bridge 相当)
- プロトコルの処理(`tinymq::` の名前空間)は lwIP にも Arduino API にも依存しません。mosquitto と paho-mqtt を相手に自動テストしています

使いみちの例:

- **ふつうの MQTT クライアントとして**: ESP32 などから、クラウドや LAN のブローカーへ
- **ボードをブローカーにする**: ESP32 / Pico W を LAN のブローカーにし、PC・スマホ・ほかのボードが TCP で、小さな MCU(CH32V など)が UDP の MQTT-SN でつながる
- **現場のゲートウェイ**: ボードのブローカーに集め、必要なトピックだけ上流のブローカーへブリッジする
- **TinyWG と組み合わせる**: MQTT-SN には暗号も認証もないので、[TinyWG](https://github.com/tanakamasayuki/TinyWG) のトンネルの中で使う

## ドキュメント

- [初心者ガイド](docs/guide-beginner.ja.md): ESP32 をクライアントにするところから、ボードをブローカーにして小さなチップを MQTT-SN でつなぐところまで
- [上級者ガイド](docs/guide-advanced.ja.md): 構成、メモリと設定、受け口の選び方、QoS とセッション、MQTT 5.0、MQTT-SN、ブリッジ、永続化、認証と認可、実行モデル
- [API リファレンス](docs/api.ja.md): すべての公開クラスとメソッド
- [仕様書](SPEC.ja.md): 設計と計測値
- [テスト](tests/README.ja.md): テストの一覧と動かし方

## サンプル

| サンプル | 内容 |
|---|---|
| [`Esp32Client`](examples/Esp32Client/README.ja.md) | ブローカーに接続し、5 秒ごとに publish、コマンドを subscribe |
| [`Esp32Broker`](examples/Esp32Broker/README.ja.md) | ESP32 をブローカーにする。TCP(lwIP)と MQTT-SN のゲートウェイ、スケッチ自身の購読 |
| [`Esp32Bridge`](examples/Esp32Bridge/README.ja.md) | ローカルのブローカーから上流のブローカーへ、トピックを付け替えてブリッジ |
| [`Esp32Persist`](examples/Esp32Persist/README.ja.md) | retain と永続セッションを LittleFS に保存し、再起動しても引き継ぐ |
| [`PicoWBroker`](examples/PicoWBroker/README.ja.md) | Pico W を Arduino の `WiFiServer` で受けるブローカーにする |
| [`Ch32SnSensor`](examples/Ch32SnSensor/README.ja.md) | Ethernet 付き CH32V から MQTT-SN で値を送り、コマンドを受ける |

## 対応状況

| コア | クライアント | ブローカー | MQTT-SN | 確認状況 |
|---|---|---|---|---|
| ESP32 / ESP32-S3 (arduino-esp32 3.3) | ○ | ○(`TinyMQLwipListener` / `TinyMQTcpListener`) | ゲートウェイ / クライアント | 実機で確認(Wi-Fi 越しの相互接続試験すべて) |
| RP2040 / RP2350 (arduino-pico 6.3、Pico W / Pico 2 W) | ○ | ○(FreeRTOS モードは `TinyMQLwipListener` 未対応) | ゲートウェイ / クライアント | ビルドのみ |
| CH32V (ArduinoCore-CH32RV 0.4、Network ライブラリ) | ○ | ○(設定で小さくして) | 主にクライアント | ビルドのみ |
| lang-ship:host(PC 上のテスト用) | ○ | ○(`TinyMQTcpListener`) | ゲートウェイ / クライアント | mosquitto / paho-mqtt と相互接続テスト済み |

`Client` / `UDP` を持つコアなら、クライアントは原則どこでも動きます(W5500 などの Ethernet ライブラリを含む)。

### フットプリント(同じ機能のスケッチに TinyMQ を足した分)

| | Flash | RAM(静的) |
|---|---|---|
| ESP32 `TinyMQClient`(既定の設定) | +11.9 KB | +4.9 KB |
| ESP32 ブローカー(TCP + MQTT-SN、既定の設定) | +32 KB | +48 KB |
| CH32V307 `TinyMQClientSn` 段階 0(QoS −1 の送信のみ) | +1.0 KB | +0.4 KB |
| CH32V307 `TinyMQClientSn` 段階 2(全部) | +6.0 KB | +1.2 KB |

ブローカーの RAM は、ほぼ接続ごとの受信バッファ(既定 8 × 1 KB)、セッションごとの送信キュー、メッセージプール(既定 16 KB)です。CH32V307 で接続 4、プール 4 KB などに縮めたブローカーは Flash 59 KB / RAM 47 KB(lwIP の約 37 KB を含む)で動く大きさです。動的確保はしません(プールを `begin()` で 1 回だけ確保する設定もあります)。

## 使い方(クライアント)

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
    // ... Wi-Fi に接続する
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

## 使い方(ブローカー)

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
    // ... Wi-Fi に接続する
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

`broker.subscribe()` / `broker.publish()` で、スケッチ自身もブローカーの中で購読・送信できます。

## 使い方(MQTT-SN クライアント)

```cpp
NetworkUDP udp;                 // ESP32 なら WiFiUDP
TinyMQClientSn sn(udp);

void setup()
{
    // ... ネットワークに接続する
    udp.begin(0);
    sn.setGateway(IPAddress(192, 168, 1, 20), 1884);
    sn.publishPredefined(1, "21.5");      // QoS -1: 接続なしで送れる
    sn.connect("ch32-1");                 // 接続すると購読や QoS 1 / 2 も使える
    sn.subscribe("cmd/#", 1);
}

void loop()
{
    sn.update();
}
```

## 設定(ビルド時)

`-D` で上書きできます(`src/tinymq/mq_config.h`)。主なもの:

| マクロ | 既定値 | |
|---|---|---|
| `TINYMQ_MAX_PACKET` | 1024 | 受け付けるパケットの最大長(クライアントのバッファ) |
| `TINYMQ_BROKER_MAX_CLIENTS` | 8 | ブローカーの同時接続数(TCP と MQTT-SN の合計) |
| `TINYMQ_BROKER_MAX_SESSIONS` | 16 | 保持するセッション数(切断中の永続セッションを含む) |
| `TINYMQ_BROKER_POOL_BYTES` | 16384 | メッセージプール。0 なら `begin()` で確保 |
| `TINYMQ_ENABLE_MQTT5` | 1 | ブローカーで 5.0 を受け付ける |
| `TINYMQ_ENABLE_QOS2_DELIVERY` | 1 | ブローカーから購読者への QoS 2 の配送 |
| `TINYMQ_SN_CLIENT_LEVEL` | 2 | MQTT-SN クライアントの機能の段階(0 / 1 / 2) |

すべての設定は[上級者ガイド](docs/guide-advanced.ja.md)にあります。

## 制限

- IPv4 のみ(MQTT-SN のゲートウェイ / クライアント)
- MQTT 5.0 の拡張認証(AUTH)は未対応。Authentication Method を含む CONNECT は 0x8C で断ります
- ブローカー側の TLS、MQTT over WebSocket、MQTT-SN 2.0 は対象外
- ブリッジが上流が落ちている間に預かるのは、QoS 1 / 2 を `TINYMQ_BRIDGE_QUEUE_BYTES`(既定 2 KB)まで。再起動では消えます
- arduino-pico の FreeRTOS モードでは `TinyMQLwipListener` を使えません(`TinyMQTcpListener` を使います)

## ライセンス

MIT。第三者のコードは含みません。プロトコルは OASIS の公開仕様(MQTT 3.1.1、MQTT 5.0、MQTT-SN 1.2)から実装しています。
