# Changelog / 変更履歴

## Unreleased
- (EN) Specification (SPEC.md) and project skeleton.
- (JA) 仕様書(SPEC.ja.md)とプロジェクトの雛形。
- (EN) Protocol core: MQTT 3.1 / 3.1.1 / 5.0 packet codec, topic matching, message pool.
- (JA) プロトコルの core: MQTT 3.1 / 3.1.1 / 5.0 のパケットのコーデック、トピックの照合、メッセージプール。
- (EN) TinyMQClient: MQTT 3.1.1 / 3.1 client over any Arduino Client (QoS 0/1/2, retain, will, persistent sessions with resend, keep alive, automatic reconnect).
- (JA) TinyMQClient: 任意の Arduino Client の上の MQTT 3.1.1 / 3.1 クライアント(QoS 0 / 1 / 2、retain、will、再送付きの永続セッション、keepalive、自動再接続)。
- (EN) TinyMQBroker: MQTT 3.1.1 / 3.1 broker with TinyMQTcpListener for any Arduino server (QoS 0/1/2 with QoS 2 delivery selectable, retained messages, wills, persistent sessions, session takeover, authentication and authorisation callbacks, local client).
- (JA) TinyMQBroker: MQTT 3.1.1 / 3.1 のブローカーと、任意の Arduino サーバーで使う TinyMQTcpListener(QoS 0 / 1 / 2、QoS 2 の配送は選択可、retain、will、永続セッション、セッションの乗っ取り、認証・認可のコールバック、ローカルクライアント)。
- (EN) MQTT-SN 1.2 gateway for the broker (TinyMQSnListener over any Arduino UDP): registration, predefined and short topics, QoS -1 to 2, wills, sleeping clients, resending over UDP.
- (JA) ブローカーの MQTT-SN 1.2 ゲートウェイ(任意の Arduino UDP の上で動く TinyMQSnListener): トピックの登録、事前定義トピックと短いトピック名、QoS -1 〜 2、will、スリープするクライアント、UDP での再送。
