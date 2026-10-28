# テスト

TinyWG と同じく、[pytest-embedded](https://docs.espressif.com/projects/pytest-embedded/en/latest/) + Arduino CLI([pytest-embedded-arduino-cli](https://github.com/tanakamasayuki/pytest-embedded-arduino-cli))で動かします。既定のプロファイルは `lang-ship:host`(PC 上で動くスケッチ)です。

```sh
cd tests
uv run pytest smoke codec topic pool sn_codec props
uv run pytest client_interop          # mosquitto が必要(下記)
uv run pytest broker_interop sn_client_interop bridge_interop
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
| `client_interop/` | `TinyMQClient`(3.1.1 / 3.1 / 5.0)をシリアルのコマンドで動かす(5.0 は `client_v5_scenarios.py`: プロパティの送受信、購読識別子、No Local、セッションの有効期限、割り当てられた Client ID と理由コード、DISCONNECT で will、Will Delay、乗っ取りの DISCONNECT、CONNACK のプロパティ(Receive Maximum、Maximum QoS、Retain Available、Server Keep Alive、Maximum Packet Size)に従うこと、許していないトピックエイリアスでの DISCONNECT、サーバーからの DISCONNECT と理由コード付きの拒否)。mosquitto 相手に QoS 0 / 1 / 2 の送受信と QoS の引き下げ、retain、購読解除、will(DISCONNECT では出ない / 切断で出る)、永続セッション、keepalive、受信バッファより大きいメッセージの読み捨て、MQTT 3.1、ユーザ名とパスワード。スクリプトで動かす偽ブローカー相手に、再接続後の QoS 1 の再送(DUP)と PUBREL の再送、セッションがないときは再送しないこと、QoS 2 の重複受信、PINGRESP が来ないときの切断、接続の拒否 | host / esp32 / esp32s3 / picow |
| `broker_interop/` | `TinyMQBroker` + `TinyMQTcpListener<WiFiServer>` をシリアルのコマンドで動かし(5.0 は `broker_v5_scenarios.py`: CONNACK のプロパティと割り当てた Client ID、拡張認証の拒否、プロパティの中継、購読識別子、No Local・Retain As Published・Retain Handling、メッセージの有効期限、セッションの有効期限、Will Delay、トピックエイリアス、理由コード、共有購読、Receive Maximum と Maximum Packet Size、サーバーからの DISCONNECT)、paho-mqtt と生ソケットのクライアントから接続。QoS 0 / 1 / 2 と重なる購読(最大の QoS で 1 回)、最大 QoS の制限、ワイルドカードと `$` トピック、retain(転送時はフラグを落とす、同じフィルタの再購読で再送、空で削除)、will(切断・keepalive 切れで出る / DISCONNECT では出ない)、永続セッション(QoS 0 は保持しない)、同じ Client ID の乗っ取り、再接続後の DUP 付き再送と PUBREL の再送、QoS 2 の重複受信、認証、トピックごとの認可、ローカルクライアント、購読解除、プロトコル違反(CONNECT 前のパケット、未知のレベル、空の Client ID、2 回目の CONNECT、予約フラグ、大きすぎるパケット、CONNECT の待ち時間切れ)、MQTT 3.1、接続数の上限、ペイロードの長さ、実機では lwIP の受け口(`TinyMQLwipListener`、ポート 1885)でも TCP と 5.0 のシナリオをもう一度流す。永続化(`saveState()` → `end()` → `loadState()` で再起動を模し、retain、永続セッションと未配送のメッセージ、5.0 の購読識別子と共有購読が戻ること)、最後に全部が解放されたことの確認。同じ DUT で MQTT-SN ゲートウェイ(`TinyMQSnListener`)も Python 製の MQTT-SN クライアント(`helpers/sn_peer.py`)から試す(`broker_sn_scenarios.py`): QoS 0 / 1 / 2 の送受信と TCP 側との相互の配送、トピック名の登録、ワイルドカード購読でゲートウェイから REGISTER、短いトピック名、事前定義トピック、未知のトピック ID、接続なしの QoS -1、will(WILLTOPIC / WILLMSG のやり取り、WILLTOPICUPD / WILLMSGUPD、keepalive 切れで出る)、keepalive、スリープ(眠っている間のメッセージを起床時に配送して PINGRESP)、UDP での再送(DUP)と応答しないクライアントの切断、QoS 2 の重複受信、永続セッション、CONNECT の再送、SEARCHGW | host / esp32 / esp32s3 / picow |
| `sn_client_interop/` | `TinyMQClientSn` をシリアルのコマンドで動かす。同じスケッチの中で動くブローカーのゲートウェイと実際の UDP ソケットでつなぎ(待っている間は `onIdle()` でブローカーを回す)、paho-mqtt で TCP 側から確かめる: 接続なしの QoS -1、QoS 0 / 1 / 2 の送信(登録は自動)、retain、事前定義と短いトピック名、購読(ワイルドカードでゲートウェイから REGISTER)、will と keepalive 切れ、スリープと起床時の受信、スリープからの CONNECT。スクリプトで動かす偽ゲートウェイ相手に、DUP 付きの再送、「不正なトピック ID」での登録し直し、応答がないときの失敗、未知のトピック ID の拒否、QoS 2 の重複受信、ゲートウェイからの DISCONNECT、接続の拒否、ゲートウェイ探索 | host / esp32 / esp32s3 / picow |
| `props/` | MQTT 5.0 のプロパティの組み立てと読み出し、検証(型、値の範囲、パケットごとに許される ID、重複、UTF-8)、指定した ID を除いたコピー | host / esp32 / esp32s3 / ch32v307 / picow |
| `bridge_interop/` | `TinyMQBridge` を 2 本(5.0 と 3.1.1)つないだブローカーを、PC の mosquitto へブリッジ。ブリッジ前の retain を上流へ送ること、out / in / both の転送とプレフィックスの付け替え、QoS の上限、5.0 のプロパティの中継、both でループしないこと、上流が落ちている間の破棄、同じポートで戻った上流への再接続(購読と retain のやり直し) | host / esp32 / esp32s3 / picow |
| `pool/` | メッセージプールの確保・解放・結合・枯渇・アライメント、2 万回のランダムな確保と解放、参照カウント付きメッセージ | 同上 |

## 実機で動かす

`.env.example` を `.env` にコピーし、シリアルポートと Wi-Fi を書きます。Wi-Fi は `build_config.toml` 経由で `WIFI_SSID` / `WIFI_PASSWORD` としてスケッチに渡ります。

```sh
uv run --env-file .env pytest smoke codec topic pool sn_codec --profiles=esp32,esp32s3
uv run --env-file .env pytest broker_interop client_interop sn_client_interop --profile esp32 -s
uv run --env-file .env pytest --profiles=all -rs     # host とつないだすべてのボード
```

`client_interop` / `broker_interop` / `sn_client_interop` は host と Wi-Fi のボード(esp32 / esp32s3 / picow)で同じシナリオを流します。スケッチは Wi-Fi につないで `READY <IP>` を出し、テストはそれを見て相手を決めます。

- `broker_interop`: ボードがブローカー(TCP 1883、MQTT-SN 1884)。PC の paho-mqtt / 生ソケット / MQTT-SN ピアがボードの IP へつなぐ
- `client_interop`: PC の mosquitto と偽ブローカーが全アドレスで待ち、ボードが PC のアドレス(ボードへの経路から求める)へつなぐ
- `sn_client_interop`: ボードの中でブローカー・ゲートウェイ・MQTT-SN クライアントが動き、クライアントはループバック(127.0.0.1)でゲートウェイへつなぐ。偽ゲートウェイは PC で動く

PC とボードが同じネットワークにいて、互いに届く必要があります(PC のファイアウォールで mosquitto のポートを塞がないこと)。

各テストの前に `output/` を消すのは、`pyproject.toml` の `arduino_reset_dirs` で行います。
