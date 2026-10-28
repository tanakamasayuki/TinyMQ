# Esp32Broker

[English](README.md) | [初心者ガイド](../../docs/guide-beginner.ja.md)

ESP32 を MQTT ブローカーにします。TCP の MQTT(3.1.1 / 5.0、1883 番)と、UDP の MQTT-SN(1.2、1884 番)を同じトピック空間で中継します。スケッチ自身も `sensors/#` を購読し、届いた値をシリアルに表示します。

## 必要なもの

- ESP32 系のボード(arduino-esp32 3.3 以降)

## 設定する項目

| 定数 | 内容 |
|---|---|
| `kWifiSsid` / `kWifiPass` | Wi-Fi |

MQTT-SN のクライアントは、事前定義トピック ID 1 で `sensors/temperature` に、接続せずに(QoS −1)送れます。

## 動かすと

```
TinyMQ Esp32Broker
Broker at 192.168.1.20
client in:  mosquitto_sub_1234
sensors/temperature = 21.5
```

PC から `mosquitto_pub -h 192.168.1.20 -t sensors/temperature -m 21.5` を送ると、スケッチが表示します。`mosquitto_sub -h 192.168.1.20 -t '#' -v` で、ほかのクライアントや MQTT-SN のクライアント([`Ch32SnSensor`](../Ch32SnSensor/README.ja.md))の送ったものが見えます。

## 注意

- TCP の受け口は lwIP の raw API の `TinyMQLwipListener` です。Arduino の `WiFiServer` で受けたいときは `TinyMQTcpListener`([`PicoWBroker`](../PicoWBroker/README.ja.md))
- MQTT-SN には暗号も認証もありません。閉じたネットワークか、TinyWG のトンネルの中で使ってください
- 接続数やメモリは `-D` で変えられます([上級者ガイド](../../docs/guide-advanced.ja.md))
