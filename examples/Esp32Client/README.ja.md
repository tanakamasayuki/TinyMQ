# Esp32Client

[English](README.md) | [初心者ガイド](../../docs/guide-beginner.ja.md)

MQTT ブローカーに接続し、5 秒ごとに `tinymq/counter` へカウンタを publish します。`tinymq/cmd` を購読し、届いたものをシリアルに表示します。再接続も含めて接続のたびに(`onConnect()`)、購読し直して `tinymq/status` へ `online`(retain)を出します。黙って切れたときは will で `offline` が出ます。

## 必要なもの

- ESP32 系のボード(arduino-esp32 3.3 以降)
- MQTT ブローカー(PC の mosquitto、別のボードで動かす [`Esp32Broker`](../Esp32Broker/README.ja.md) など)

## 設定する項目

| 定数 | 内容 |
|---|---|
| `kWifiSsid` / `kWifiPass` | Wi-Fi |
| `kBroker` / `kBrokerPort` | ブローカーのアドレスとポート |

## 動かすと

```
TinyMQ Esp32Client
Local IP: 192.168.1.50
connected
received tinymq/cmd: hello
```

PC で `mosquitto_sub -h <ブローカー> -t 'tinymq/#' -v` を実行するとカウンタが見え、`mosquitto_pub -h <ブローカー> -t tinymq/cmd -m hello` でボードに届きます。

## 注意

- 切れたら 5 秒ごとにつなぎ直します(`setReconnectInterval()`)
- MQTT 5.0 を使うときは `mqtt.setProtocolVersion(tinymq::kMqtt5)` を `connect()` の前に呼びます
