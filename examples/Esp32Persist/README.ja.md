# Esp32Persist

[English](README.md) | [上級者ガイド](../../docs/guide-advanced.ja.md)

retain メッセージと永続セッション(購読と、まだ届けていない QoS 1 / 2 のメッセージ)を LittleFS に保存し、再起動しても引き継ぐブローカーです。retain が変わってから 2 秒後と、5 分ごとに保存します。

## 必要なもの

- ESP32 系のボード(arduino-esp32 3.3 以降)。LittleFS のパーティションがある設定

## 設定する項目

| 定数 | 内容 |
|---|---|
| `kWifiSsid` / `kWifiPass` | Wi-Fi |
| `kStateFile` | 保存するファイル |

## 動かすと

```
TinyMQ Esp32Persist
Broker at 192.168.1.20
state loaded
state saved
```

`mosquitto_pub -h 192.168.1.20 -t room/temp -m 21 -r` のあとにボードをリセットしても、`mosquitto_sub -h 192.168.1.20 -t room/temp` に `21` が届きます。

## 注意

- 新しいファイルに書いてから入れ替えるので、保存中にリセットされても前の状態が残ります
- 保存した時点で送信中だった QoS 1 / 2 は、読み戻したあと最初から送り直します(QoS 1 は重複がありえます)。遅延中の will は保存しません
- Flash の書き換え回数を考えて、保存の間隔を決めてください
