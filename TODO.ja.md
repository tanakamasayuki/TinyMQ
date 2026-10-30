# TODO / 引き継ぎメモ

最終更新: 2026-10-28。仕様は [SPEC.ja.md](SPEC.ja.md) にあります。

## 現状

- Phase 0〜4 まで済み: MQTT 3.1.1 / 3.1 のクライアント(`TinyMQClient`)とブローカー(`TinyMQBroker` + `TinyMQTcpListener`)、MQTT-SN 1.2 のゲートウェイ(`TinyMQSnListener`)とクライアント(`TinyMQClientSn`)
- host の試験(`smoke` `codec` `topic` `pool` `sn_codec` `client_interop` `broker_interop` `sn_client_interop`)はすべて通る
- 実機(2026-10-28): ESP32(D0WDQ6)/ ESP32-S3 で単体テストと、Wi-Fi 越しの `broker_interop`(29 シナリオ)、`client_interop`(12)、`sn_client_interop`(8)が通過。Pico W はビルドのみ。CH32V307 はつながっていなかったので未実施
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
  - [ ] Eclipse paho.mqtt.testing の相互接続試験スイートを当てる。仕組み(`broker_conformance/`、`tools/fetch_paho_testing.sh`、`helpers/run_paho_suite.py`)はできたが、外部のコードの実行が自動モードで止められたので未実行。`TINYMQ_RUN_PAHO_TESTING=1 uv run pytest broker_conformance -s` で動かす
  - [x] `$SYS` トピック(`setSysInterval()`、mosquitto に合わせた `$SYS/broker/...`、既定は出さない)
  - [ ] 書き込みが詰まったときの扱い(今は Arduino の `Client::write()` がブロックする前提。lwIP 版の受け口で `writable()` を使う)
- [ ] Phase 4: MQTT-SN
  - [x] コーデック(`tinymq::sn`)と単体テスト
  - [x] ゲートウェイ(`tinymq::SnGateway`、`TinyMQSnListener`)。MQTT-SN のクライアント 1 つにブローカーの接続を 1 本割り当て、パケットを相互に変換する方式。Python の MQTT-SN ピアで試験
  - [x] `TinyMQClientSn`(段階を選べるクライアント、手順はブロッキング)。自前のゲートウェイと偽ゲートウェイで試験。CH32V307 で段階ごとのフットプリントを計測(SPEC §4.5)
  - [ ] ほかの MQTT-SN 実装(Eclipse Paho MQTT-SN Gateway など)との相互接続
  - [ ] Forwarder Encapsulation、送信元アドレスの許可リスト
- [ ] Phase 5: 実機
  - [x] ESP32 / ESP32-S3: 単体テストと 3 つの相互接続試験(Wi-Fi)
  - [ ] ESP32 の `sn_client_interop` の `resend` が 1 回だけ失敗した(偽ゲートウェイが最初に REGISTER ではなく PUBLISH を受けた)。再実行では通り、ESP32-S3 でも通る。原因は未特定。再発したら受信内容がアサーションに出る
  - [x] ESP32 / ESP32-S3 で 5.0 を含む 3 つの相互接続試験が通過(2026-10-28)
  - [ ] ESP32-S3 が試験中に 1 回だけ電源投入リセットでダウンロードモードに入った(`rst:0x1 (POWERON), boot:0x23`)。再実行では起きていない。USB の電源か、ほかのスイートとの取り合いかは未確認
  - [ ] CH32V307(Ethernet。小さい設定のブローカーと lwIP の受け口でビルドは通る: Flash 59 KB / RAM 47 KB、うち lwIP 約 37 KB、Network ライブラリ)。ブローカーの軽い構成と MQTT-SN クライアント。ベンチのロックを取って使う
  - [ ] Pico W(つながっていない)
  - [x] ブローカーの遅延と速さ(`bench_broker`、ESP32-S3。SPEC §5.2)。途中で `TinyMQLwipListener` の 3 つの問題(遅延 ACK、送信キューのセグメント数、セグメントのまとめ)を直した
  - [ ] ESP32(D0WDQ6)での計測。ほかのスイートが使っていて書き込みに失敗した。組の相手(1-1)のロックも取ってやり直す
  - [ ] 接続数ごとの RAM の計測、TinyWG との組み合わせ
- [ ] Phase 6: MQTT 5.0
  - [x] プロパティのコーデック
  - [x] ブローカー(セッション / メッセージの有効期限、Will Delay、購読識別子と購読オプション、共有購読、受信側のトピックエイリアス、Receive Maximum、Maximum Packet Size、理由コード)
  - [x] クライアント(CONNECT / CONNACK のプロパティ、プロパティ付きの publish、購読オプションと識別子、理由コード付きの DISCONNECT)。mosquitto と偽ブローカーで試験
  - [ ] 拡張認証(AUTH)。今は Authentication Method を CONNACK 0x8C で断る
- [x] Phase 7: ブリッジ(`TinyMQBridge`)。ブローカーのタップ(自分の入れたものを受けない内部購読者)と `TinyMQClient` で、mosquitto の bridge 相当(out / in / both、プレフィックス、QoS)。再接続時に上流の購読とローカルの retain をやり直す
  - [x] 上流が落ちている間の QoS 1 / 2 を預かり、戻ったら順に送る(`TINYMQ_BRIDGE_QUEUE_BYTES`)
- [ ] Phase 8: lwIP の受け口、永続化のフック
  - [x] 永続化: `saveState()` / `loadState()`(保存先は `StateSink` / `StateSource` でスケッチが選ぶ)、retain の変化のコールバック、`end()`
  - [x] LittleFS に保存するサンプル(`Esp32Persist`)
  - [x] lwIP の受け口(`TinyMQLwipListener`)。コールバックは受信を溜めるだけ、ブローカーは `loop()` から lwIP のロックの下で動く。`writable()` は送信バッファの空き。ESP32 / ESP32-S3 の実機で TCP と 5.0 のシナリオが通過、Pico W と CH32V307 はビルドのみ
- [ ] 既定値と設定のプリセットを実測で決める

## ドキュメント

- [x] README、初心者ガイド、上級者ガイド、API リファレンス(日英)、サンプル 6 本(日英の README)、`keywords.txt`
- [ ] サンプルの実機での確認(今はビルドのみ。`Esp32Client` / `Esp32Broker` などは相互接続試験と同じ使い方)
- [ ] `tests/README` の英語版

## 未決定(TBD)

- 複数タスクから呼ぶ場合の排他(ESP32 でブローカーを別タスクに置く形)
- CH32V でブローカーを動かす軽量構成をどこまで用意するか
- 5.0 の拡張認証(AUTH)、MQTT 3.1(`MQIsdp`)の受け付け、`$SYS` の内容
- MQTT-SN の Forwarder Encapsulation
- ブリッジで複数の上流を扱うか

## 環境メモ

- **実機の試験**: `tests/.env` は TinyWG と同じボード(`/run/board-identify/by-id/...`)と Wi-Fi を使う。ボードはほかのスイートと共有で、シリアルはプラグインが排他する
- **常駐ベンチでの名前と組**(oep-bench の台帳、2026-10-30 に [oep] から): `esp32` = 1-2 `esp32-irrx`(`esp32-d0wdq6-ec62608443a8`)、`esp32s3` = 1-3 `s3-irrx`(`esp32-s3-d0cf1359101c`)、`ch32v307` = 3-2 `v307`(`ch32v307vct6-7bbed00a9c1c5054`、ch32rv からは `wchlink://38EF8F06BDC2`)
  - 1-2 は 1-1 と IR の組、1-3 は 1-4 と IR と GPIO19/20 の直結の組。組の相手の試験の firmware が IR や GPIO19/20 を動かすことがあるので、**組の相手の機器ロックも取る**規則(pytest では peer として宣言)。TinyMQ は Wi-Fi だけだが、1-1 / 1-4 のロックも取ると安全。まだ対応していない
  - 3-2 は WCH-Link の機材なので、機器ロックに加えて `/home/mt/dev_oep/oep-bench/locks/bench.lock` も取る
  - 2026-10-28 の 1-2 への書き込み失敗(`Unable to verify flash chip connection`)と 1-3 の POWERON リセットは、組の相手のロックを取らずに EspUsbDevice-v3 / ESP32IRPulseKit と同時に動かしたためと考えられる(同じ機材への書き込みが重なるとこの失敗になる)

- **mosquitto**: この PC には入っていない(sudo にパスワードが要る)。`tests/tools/fetch_mosquitto.sh` で `tests/.tools/mosquitto` に展開して使っている(git 管理外)

- host コア(lang-ship:host 1.7.1)には `WiFiClient` / `WiFiServer` / `WiFiUDP` があり、実ソケットで動く(`WiFiServer` は ESP32 と同じく `available()` で受け付ける)。相互接続試験はこれで PC 上の mosquitto / paho-mqtt とつなぐ
- テスト環境やボードの扱いは TinyWG の `TODO.ja.md` の「環境メモ」と同じ(`tests/.env`、`/run/board-identify/by-id/`、CH32 を使うときのベンチのロックなど)
