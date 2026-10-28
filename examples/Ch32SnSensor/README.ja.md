# Ch32SnSensor

[English](README.md) | [初心者ガイド](../../docs/guide-beginner.ja.md)

Ethernet 付きの CH32V を MQTT-SN のクライアントにします。10 秒ごとに起動からの秒数を `sensors/uptime` に publish し、`cmd/led` の `on` / それ以外で LED を切り替えます。ゲートウェイにつながっていないときは、事前定義トピック ID 1 に QoS −1(接続なし)で送ります。

## 必要なもの

- Ethernet のある CH32V(CH32V307 など。ArduinoCore-CH32RV 0.4 以降、Network ライブラリ)
- MQTT-SN のゲートウェイ([`Esp32Broker`](../Esp32Broker/README.ja.md) など)

## 設定する項目

| 定数 | 内容 |
|---|---|
| `kGateway` / `kGatewayPort` | ゲートウェイのアドレスとポート |

## 動かすと

```
TinyMQ Ch32SnSensor
Local IP: 192.168.1.40
connected
led on
```

ブローカー側で `mosquitto_sub -h <ブローカー> -t 'sensors/#' -v` を実行すると値が見え、`mosquitto_pub -h <ブローカー> -t cmd/led -m on` で LED が点きます。

## 注意

- `connect()`、`subscribe()`、QoS 1 の `publish()` は、ゲートウェイの応答を待ちます(ブロッキング)
- `-DTINYMQ_SN_CLIENT_LEVEL=0` にすると QoS −1 の送信だけになり、さらに小さくなります([上級者ガイド](../../docs/guide-advanced.ja.md))
- 実機での確認はまだです(ビルドのみ)
