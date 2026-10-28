# PicoWBroker

[English](README.md) | [初心者ガイド](../../docs/guide-beginner.ja.md)

Pico W / Pico 2 W を MQTT ブローカーにします。Arduino の `WiFiServer` で受け付け(`TinyMQTcpListener`)、スケッチ自身が `#` を購読して、届いたものをすべてシリアルに表示します。

## 必要なもの

- Pico W / Pico 2 W(arduino-pico 6.3 以降、FreeRTOS モードでない設定)

## 設定する項目

| 定数 | 内容 |
|---|---|
| `kWifiSsid` / `kWifiPass` | Wi-Fi |

## 動かすと

```
TinyMQ PicoWBroker
Broker at 192.168.1.30
test/x = hello
```

`mosquitto_pub -h 192.168.1.30 -t test/x -m hello` を送ると表示されます。

## 注意

- `TinyMQTcpListener` は、`accept()` か `available()` を持つ Arduino のサーバーならどれでも使えます(`WiFiServer`、`NetworkServer`、`EthernetServer` など)。lwIP の raw API で受けたいときは `TinyMQLwipListener`
- 実機での確認はまだです(ビルドのみ)
