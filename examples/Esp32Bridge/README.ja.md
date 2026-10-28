# Esp32Bridge

[English](README.md) | [上級者ガイド](../../docs/guide-advanced.ja.md)

ESP32 のブローカーを上流のブローカーへブリッジします。ローカルの `sensors/#` を上流へ `site1/sensors/#` として送り、上流の `site1/cmd/#` をローカルのクライアントへ `cmd/#` として届けます。

## 必要なもの

- ESP32 系のボード(arduino-esp32 3.3 以降)
- 上流の MQTT ブローカー(クラウドや LAN の mosquitto など)

## 設定する項目

| 定数 | 内容 |
|---|---|
| `kWifiSsid` / `kWifiPass` | Wi-Fi |
| `kUpstreamHost` / `kUpstreamPort` | 上流のブローカー |

## 動かすと

10 秒ごとに、上流とのつながりと転送の数を表示します。

```
TinyMQ Esp32Bridge
Broker at 192.168.1.20
upstream up, out 3, in 1, dropped 0
```

ローカルで `mosquitto_pub -h 192.168.1.20 -t sensors/t -m 1` を送ると、上流の `site1/sensors/t` に届きます。

## 注意

- 上流と 5.0 でつなぐと No Local で購読するので、`kBoth` のトピックでもループしません。3.1.1 では、両側で別のプレフィックスにしてください
- 上流が落ちている間に外へ出す QoS 1 / 2 はブリッジが預かり、つながったら送ります。QoS 0 と預かりきれないものは捨てて数えます(`dropped`)。つながるたびに、上流の購読とローカルの retain を送り直します
- TLS は `WiFiClient` を `WiFiClientSecure` にして、証明書を設定します
