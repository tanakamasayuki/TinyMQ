# テスト

TinyWG と同じく、[pytest-embedded](https://docs.espressif.com/projects/pytest-embedded/en/latest/) + Arduino CLI([pytest-embedded-arduino-cli](https://github.com/tanakamasayuki/pytest-embedded-arduino-cli))で動かします。既定のプロファイルは `lang-ship:host`(PC 上で動くスケッチ)です。

```sh
cd tests
uv run pytest smoke codec topic pool sn_codec
uv run pytest client_interop          # mosquitto が必要(下記)
uv run pytest broker_interop
```

## mosquitto

相互接続テストは PC 上の mosquitto を相手にします。`$MOSQUITTO`、`PATH`(`/usr/sbin` を含む)、`tests/.tools/mosquitto` の順に探し、見つからなければスキップします。root がなくても、Debian / Ubuntu なら次のコマンドでパッケージを `tests/.tools/mosquitto` に展開して使えます(インストールはしません)。

```sh
tests/tools/fetch_mosquitto.sh
```

テストごとに空いているポートで mosquitto を起動し、設定とログは各テストの `output/` に残ります。

## 各テスト

| ディレクトリ | 内容 | プロファイル |
|---|---|---|
| `smoke/` | ビルドとハーネスの確認 | host / esp32 / esp32s3 / ch32v307 / picow |
| `codec/` | 可変長整数、UTF-8 の検証、固定ヘッダ、全パケット種別のエンコード / デコード(3.1.1 と 5.0)を仕様書から組み立てたバイト列と照合。不正なパケットの拒否、バッファ不足 | 同上 |
| `sn_codec/` | MQTT-SN 1.2 のフラグ、長さの短い形式と長い形式(256 オクテットの境界)、ゲートウェイとクライアントが使う全メッセージを仕様書から組み立てたバイト列と照合。不正なメッセージの拒否 | host / esp32 / esp32s3 / ch32v307 / picow |
| `topic/` | トピック名とフィルタの検証、照合(仕様書 §4.7 の例)、`$` で始まるトピック、共有購読 `$share/` の分解 | 同上 |
| `client_interop/` | `TinyMQClient`(3.1.1 / 3.1)をシリアルのコマンドで動かす。mosquitto 相手に QoS 0 / 1 / 2 の送受信と QoS の引き下げ、retain、購読解除、will(DISCONNECT では出ない / 切断で出る)、永続セッション、keepalive、受信バッファより大きいメッセージの読み捨て、MQTT 3.1、ユーザ名とパスワード。スクリプトで動かす偽ブローカー相手に、再接続後の QoS 1 の再送(DUP)と PUBREL の再送、セッションがないときは再送しないこと、QoS 2 の重複受信、PINGRESP が来ないときの切断、接続の拒否 | host のみ |
| `broker_interop/` | `TinyMQBroker` + `TinyMQTcpListener<WiFiServer>` をシリアルのコマンドで動かし、paho-mqtt と生ソケットのクライアントから接続。QoS 0 / 1 / 2 と重なる購読(最大の QoS で 1 回)、最大 QoS の制限、ワイルドカードと `$` トピック、retain(転送時はフラグを落とす、同じフィルタの再購読で再送、空で削除)、will(切断・keepalive 切れで出る / DISCONNECT では出ない)、永続セッション(QoS 0 は保持しない)、同じ Client ID の乗っ取り、再接続後の DUP 付き再送と PUBREL の再送、QoS 2 の重複受信、認証、トピックごとの認可、ローカルクライアント、購読解除、プロトコル違反(CONNECT 前のパケット、未知のレベル、空の Client ID、2 回目の CONNECT、予約フラグ、大きすぎるパケット、CONNECT の待ち時間切れ)、MQTT 3.1、接続数の上限、ペイロードの長さ、最後に全部が解放されたことの確認 | host のみ |
| `pool/` | メッセージプールの確保・解放・結合・枯渇・アライメント、2 万回のランダムな確保と解放、参照カウント付きメッセージ | 同上 |

## 実機で動かす

`.env.example` を `.env` にコピーし、シリアルポートと Wi-Fi を書きます。

```sh
uv run --env-file .env pytest smoke --profile esp32 -s
```
