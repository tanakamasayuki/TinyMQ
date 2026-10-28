# TODO / 引き継ぎメモ

最終更新: 2026-10-28。仕様は [SPEC.ja.md](SPEC.ja.md) にあります。

## 現状

- Phase 0〜4 まで済み: MQTT 3.1.1 / 3.1 のクライアント(`TinyMQClient`)とブローカー(`TinyMQBroker` + `TinyMQTcpListener`)、MQTT-SN 1.2 のゲートウェイ(`TinyMQSnListener`)とクライアント(`TinyMQClientSn`)
- host の試験(`smoke` `codec` `topic` `pool` `sn_codec` `client_interop` `broker_interop` `sn_client_interop`)はすべて通る。ESP32 / Pico W / CH32V307 はビルドのみで、実機ではまだ動かしていない
- ブランチは `main` のみ。push はまだ
- 決まったこと(詳細は SPEC)
  - クライアントとブローカーを 1 つのライブラリに入れ、使う側だけを選ぶ
  - TCP は MQTT 3.1.1 / 5.0。ブローカーはなるべく仕様の全体を実装し、設定で軽くできるようにする
  - UDP は MQTT-SN 1.2。ブローカーに組み込んだゲートウェイと、段階を選べるクライアント
  - QoS 2 は受信を常に受け付け、配送はビルド時・実行時に落とせる
  - 輸送は外部のもの(`Client&` / `Server` / `UDP&`)を使う形を先に作り、lwIP 直叩きは後で足す
  - ブリッジ(上流のブローカーへの転送)もやる
  - MIT、スクラッチ実装

## 残っている作業

SPEC §7 の順に進める。

- [x] Phase 0: 雛形(`library.properties`、`src/`、`tests/`、`tools/bump_version.py`、`.github/workflows/release.yml` をリリースツールキットから)
- [x] Phase 1: core(3.1.1 / 5.0 のコーデック、トピック照合、メッセージプール)と host の単体テスト。5.0 のプロパティは生のまま保持するところまで(解釈は Phase 6)。購読表はセッションと一緒に Phase 3 で作る
- [x] Phase 2: `TinyMQClient`(3.1.1 / 3.1)と mosquitto・偽ブローカーとの相互接続。プロトコルの処理は `tinymq::ClientEngine`(Arduino 非依存)、`TinyMQClient` は `Client&` のアダプタ
- [x] Phase 3: `TinyMQBroker`(3.1.1 / 3.1)+ `TinyMQTcpListener`、ローカルクライアント、認証・認可のコールバック、paho-mqtt と生ソケットでの試験
  - [ ] Eclipse paho.mqtt.testing の相互接続試験スイートを当てる
  - [ ] `$SYS` トピック
  - [ ] 書き込みが詰まったときの扱い(今は Arduino の `Client::write()` がブロックする前提。lwIP 版の受け口で `writable()` を使う)
- [ ] Phase 4: MQTT-SN
  - [x] コーデック(`tinymq::sn`)と単体テスト
  - [x] ゲートウェイ(`tinymq::SnGateway`、`TinyMQSnListener`)。MQTT-SN のクライアント 1 つにブローカーの接続を 1 本割り当て、パケットを相互に変換する方式。Python の MQTT-SN ピアで試験
  - [x] `TinyMQClientSn`(段階を選べるクライアント、手順はブロッキング)。自前のゲートウェイと偽ゲートウェイで試験。CH32V307 で段階ごとのフットプリントを計測(SPEC §4.5)
  - [ ] ほかの MQTT-SN 実装(Eclipse Paho MQTT-SN Gateway など)との相互接続
  - [ ] Forwarder Encapsulation、送信元アドレスの許可リスト
- [ ] Phase 5: 実機(ESP32 / ESP32-S3 / Pico W / CH32V307)、計測、TinyWG との組み合わせ
- [ ] Phase 6: MQTT 5.0
- [ ] Phase 7: ブリッジ
- [ ] Phase 8: lwIP の受け口、永続化のフック
- [ ] 既定値と設定のプリセットを実測で決める

## 未決定(TBD)

- 複数タスクから呼ぶ場合の排他(ESP32 でブローカーを別タスクに置く形)
- CH32V でブローカーを動かす軽量構成をどこまで用意するか
- 5.0 の拡張認証(AUTH)、MQTT 3.1(`MQIsdp`)の受け付け、`$SYS` の内容
- MQTT-SN の Forwarder Encapsulation
- ブリッジで複数の上流を扱うか

## 環境メモ

- **mosquitto**: この PC には入っていない(sudo にパスワードが要る)。`tests/tools/fetch_mosquitto.sh` で `tests/.tools/mosquitto` に展開して使っている(git 管理外)

- host コア(lang-ship:host 1.7.1)には `WiFiClient` / `WiFiServer` / `WiFiUDP` があり、実ソケットで動く(`WiFiServer` は ESP32 と同じく `available()` で受け付ける)。相互接続試験はこれで PC 上の mosquitto / paho-mqtt とつなぐ
- テスト環境やボードの扱いは TinyWG の `TODO.ja.md` の「環境メモ」と同じ(`tests/.env`、`/run/board-identify/by-id/`、CH32 を使うときのベンチのロックなど)
