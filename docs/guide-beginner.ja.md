# 初心者ガイド

[English](guide-beginner.md) | [README](../README.ja.md) | [上級者ガイド](guide-advanced.ja.md)

ESP32 を MQTT のクライアントにして PC のブローカーとつなぐところから始め、ESP32 をブローカーにして、小さなマイコンを MQTT-SN でつなぐところまで進みます。

## 1. MQTT と TinyMQ

MQTT は、**ブローカー**を真ん中に置いてメッセージをやり取りする仕組みです。

- 送る側は**トピック**(`sensors/room1/temp` のような名前)を付けて **publish** します
- 受け取りたい側は、トピックを指定して **subscribe**(購読)しておきます
- ブローカーは、届いたメッセージを、そのトピックを購読しているクライアント全員に配ります

送る側と受け取る側は、お互いを知らなくてかまいません。購読では `+`(1 階層)と `#`(それ以下すべて)のワイルドカードが使えます。`sensors/+/temp` は `sensors/room1/temp` に、`sensors/#` は `sensors` の下すべてに当てはまります。

TinyMQ には次の部品があります。

| 部品 | 役割 |
|---|---|
| `TinyMQClient` | MQTT クライアント(TCP) |
| `TinyMQBroker` | MQTT ブローカー |
| `TinyMQSnListener` | ブローカーに付ける MQTT-SN のゲートウェイ(UDP) |
| `TinyMQClientSn` | MQTT-SN クライアント(UDP)。小さなマイコン向け |
| `TinyMQBridge` | ブローカーから上流のブローカーへのブリッジ |

MQTT-SN は、MQTT を UDP で小さな機器に使うための規格です。トピック名の代わりに 2 バイトの ID を使うので、パケットが小さく、TCP がなくても動きます。

## 2. 用意するもの

- ESP32 のボード(ESP32-S3 などでも同じ)と Arduino IDE または Arduino CLI。arduino-esp32 3.3 以降
- PC(Linux、macOS、Windows)。MQTT ブローカーの mosquitto を入れます
- 後半で使うなら、Ethernet 付きの CH32V(CH32V307 など)

## 3. TinyMQ を入れる

ライブラリのフォルダに置きます。

```sh
cd ~/Arduino/libraries      # Windows なら ドキュメント\Arduino\libraries
git clone https://github.com/tanakamasayuki/TinyMQ
```

Arduino IDE を再起動すると、「ファイル → スケッチ例 → TinyMQ」にサンプルが出ます。

## 4. PC にブローカーを用意する

mosquitto を入れます。

```sh
sudo apt install mosquitto mosquitto-clients   # Debian / Ubuntu
brew install mosquitto                          # macOS
```

mosquitto 2.0 以降は、既定では自分の PC からの接続しか受け付けません。ほかの機器から入れるように、設定ファイル(例 `tinymq.conf`)を作ります。

```
listener 1883
allow_anonymous true
```

これは試すためだけの設定です(誰でも接続できます)。起動します。

```sh
mosquitto -c tinymq.conf -v
```

もう 1 つ端末を開いて、すべてのトピックを見張っておきます。

```sh
mosquitto_sub -h localhost -t '#' -v
```

## 5. ESP32 をクライアントにする

スケッチ例の [`Esp32Client`](../examples/Esp32Client/README.ja.md) を開き、次を書き換えます。

```cpp
static const char *kWifiSsid = "your-ssid";
static const char *kWifiPass = "your-passphrase";
static const IPAddress kBroker(192, 168, 1, 10);   // PC のアドレス
```

書き込んでシリアルモニタ(115200 bps)を開くと、次のように出ます。

```
TinyMQ Esp32Client
Local IP: 192.168.1.50
connected
```

`mosquitto_sub` の端末に、5 秒ごとにカウンタが出ます。

```
tinymq/status online
tinymq/counter 0
tinymq/counter 1
```

PC からボードへ送ってみます。

```sh
mosquitto_pub -h localhost -t tinymq/cmd -m hello
```

シリアルに `received tinymq/cmd: hello` と出れば成功です。

### スケッチの中身

```cpp
WiFiClient net;              // TCP の接続(TLS なら WiFiClientSecure)
TinyMQClient mqtt(net);

mqtt.setServer(kBroker, 1883);
mqtt.setClientId("esp32-client");   // ブローカーの中で一意な名前
mqtt.onMessage(onMessage);          // 届いたときに呼ばれる関数
mqtt.connect();                     // 接続して返事を待つ
mqtt.subscribe("tinymq/cmd", 1);

// loop() の中
mqtt.update();                      // 受信と keepalive。必ず呼ぶ
mqtt.publish("tinymq/counter", text, 1);
```

`update()` を呼ばないと、受信もできず、keepalive が切れてブローカーに切断されます。`loop()` の中で `delay()` を長く入れないようにします。

## 6. QoS、retain、will

| 機能 | 内容 |
|---|---|
| QoS 0 | 送りっぱなし。届かないこともある |
| QoS 1 | 確認が来るまで送り直す。2 回届くことがある |
| QoS 2 | ちょうど 1 回届ける。いちばん重い |
| retain | ブローカーがそのトピックの最後の値を覚えておき、あとから購読したクライアントにもすぐ渡す |
| will | クライアントが黙って消えたとき(電源断など)、ブローカーが代わりに出すメッセージ |

`Esp32Client` は、接続すると `tinymq/status` に `online` を retain で出し、will に `offline` を登録しています。ボードの電源を抜くと、keepalive が切れたところで `mosquitto_sub` に `tinymq/status offline` が出ます。

同じトピックに空のメッセージを retain で送ると、覚えている値が消えます。

```sh
mosquitto_pub -h localhost -t tinymq/status -n -r
```

## 7. ESP32 をブローカーにする

PC の代わりに、ESP32 をブローカーにします。[`Esp32Broker`](../examples/Esp32Broker/README.ja.md) を書き込むと、シリアルにアドレスが出ます。

```
TinyMQ Esp32Broker
Broker at 192.168.1.20
```

PC の mosquitto は止めて、PC からはクライアントとしてつなぎます。

```sh
mosquitto_sub -h 192.168.1.20 -t '#' -v
mosquitto_pub -h 192.168.1.20 -t sensors/temperature -m 21.5
```

`sensors/` の下に送ると、スケッチ自身も購読しているので、シリアルにも出ます。

```
client in:  mosquitto_pub_1234
sensors/temperature = 21.5
```

5 の `Esp32Client` の `kBroker` をこのボードのアドレスにすれば、ボード同士でもつながります。

### スケッチの中身

```cpp
TinyMQBroker broker;
TinyMQLwipListener tcp(1883);   // TCP の受け口
WiFiUDP udp;
TinyMQSnListener sn(udp);       // MQTT-SN の受け口(UDP)

broker.begin();
tcp.begin();
udp.begin(1884);
broker.addListener(tcp);
broker.addListener(sn);
broker.onMessage(onMessage);    // スケッチの購読に届いたもの
broker.subscribe("sensors/#");

// loop() の中
broker.update();                // これだけ
```

ブローカーは、受け口(listener)を登録して使います。TCP の MQTT、UDP の MQTT-SN、上流へのブリッジは、どれも同じブローカーのトピックを共有します。

## 8. 小さなマイコンを MQTT-SN でつなぐ

CH32V のように RAM の少ないマイコンでは、MQTT-SN のクライアント `TinyMQClientSn` を使います。[`Ch32SnSensor`](../examples/Ch32SnSensor/README.ja.md) の `kGateway` を 7 のボードのアドレスにして書き込みます。

```cpp
static const IPAddress kGateway(192, 168, 1, 20);
```

`mosquitto_sub -h 192.168.1.20 -t '#' -v` の端末に、10 秒ごとに値が出ます。

```
sensors/uptime 10
sensors/uptime 20
```

PC から LED を点けます。

```sh
mosquitto_pub -h 192.168.1.20 -t cmd/led -m on
```

### 接続しないで送る(QoS −1)

MQTT-SN には、接続しないで送れる QoS −1 があります。トピックは、ゲートウェイと前もって決めておいた ID(事前定義トピック)か、2 文字の短い名前で指定します。`Esp32Broker` は ID 1 を `sensors/temperature` にしています。

```cpp
sn.setGateway(kGateway, 1884);
sn.publishPredefined(1, "21.5");   // 接続しなくてよい
```

送るだけの機器なら、これだけで足ります。`-DTINYMQ_SN_CLIENT_LEVEL=0` でビルドすると、ほかの機能が入らず Flash 1 KB ほどになります。

## 9. 安全のために

- ここまでの設定は、誰でも接続できる状態です。実際に使うときは、ブローカーに認証(`broker.onAuthenticate()`)と認可(`broker.onAuthorize()`)を付けてください。[上級者ガイド](guide-advanced.ja.md) §9
- MQTT-SN には暗号も認証もありません。閉じたネットワークで使うか、[TinyWG](https://github.com/tanakamasayuki/TinyWG) の WireGuard のトンネルの中で使ってください
- TCP のクライアントで暗号化するには、`WiFiClient` の代わりに `WiFiClientSecure` を渡し、ブローカーの証明書を設定します

## 10. うまくいかないとき

| 症状 | 見るところ |
|---|---|
| `connect failed, reason 0x80` | ブローカーのアドレスとポート、PC のファイアウォール、mosquitto が全アドレスで待っているか(`listener 1883`) |
| `connect failed, reason 0x87` | ブローカーが認証を求めている(`allow_anonymous`、ユーザ名とパスワード) |
| `connect failed, reason 0x85` | Client ID が空、または受け付けられない |
| つながるがすぐ切れる | `loop()` で `update()` を呼んでいるか。`delay()` が長すぎないか。同じ Client ID の機器がほかにないか(後からつないだ方が残る) |
| 届かない | トピックのつづり、購読のワイルドカード。`mosquitto_sub -t '#' -v` で実際に流れているものを見る |
| 大きなメッセージだけ届かない | クライアントの受信バッファ(`TINYMQ_MAX_PACKET`、既定 1024 バイト)より大きいものは捨てる |
| MQTT-SN がつながらない | ゲートウェイのアドレスとポート(1884)、ブローカー側で `udp.begin(1884)` と `addListener(sn)` をしているか |

`lastReason()` の値の意味は [API リファレンス](api.ja.md#理由コード)にあります。
