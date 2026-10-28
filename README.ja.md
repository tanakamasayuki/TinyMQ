# TinyMQ

[English](README.md)

Arduino 向けの、コアに依存しない MQTT ライブラリです。**クライアントとブローカーの両方**を含みます。

- `TinyMQClient`: 任意の Arduino `Client` の上で MQTT 3.1.1 / 5.0
- `TinyMQClientSn`: 任意の Arduino `UDP` の上で MQTT-SN 1.2。TCP が重い小さなチップ向け
- `TinyMQBroker`: TCP の MQTT と UDP の MQTT-SN のクライアントを同じトピック空間で中継する。設定で機能を落として軽くできる

> 開発中です。設計は [SPEC.ja.md](SPEC.ja.md)、進み具合は [TODO.ja.md](TODO.ja.md) を見てください。

## ライセンス

MIT
