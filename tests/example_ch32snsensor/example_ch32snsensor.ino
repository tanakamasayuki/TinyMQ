// Generated from examples/Ch32SnSensor/Ch32SnSensor.ino by tests/tools/sync_examples.py:
// do not edit. Settings come from tests/.env (build_config.toml).
// en: CH32V with Ethernet as an MQTT-SN client: publish a reading every 10 s and switch an LED by "cmd/led".
// ja: Ethernet 付き CH32V を MQTT-SN のクライアントにする。10 秒ごとに値を publish し、"cmd/led" で LED を切り替える。

/* requires: ETH_MAC */

#include <Network.h>
#include <TinyMQ.h>

// en: The MQTT-SN gateway (a board running Esp32Broker, for example).
// ja: MQTT-SN のゲートウェイ(Esp32Broker を動かすボードなど)。
static const IPAddress kGateway(HOST_IP);
static constexpr uint16_t kGatewayPort = 1884;

static NetworkUDP udp;
static TinyMQClientSn sn(udp);

static void onMessage(const tinymq::SnMessage &m, void *)
{
    bool on = m.payloadLen == 2 && memcmp(m.payload, "on", 2) == 0;
#ifdef LED_BUILTIN
    digitalWrite(LED_BUILTIN, on ? HIGH : LOW);
#endif
    Serial.print("led ");
    Serial.println(on ? "on" : "off");
}

// en: Connect and subscribe; again after the gateway was lost, since the new session has no subscriptions.
// ja: 接続して購読する。ゲートウェイを見失ったあとも同じで、新しいセッションには購読がない。
static void connectAndSubscribe()
{
    if (sn.connect() && sn.subscribe("cmd/led", 1) >= 0)
        Serial.println("connected");
    else
        Serial.println("no gateway");
}

void setup()
{
    Serial.begin(115200);
    delay(100);
    Serial.println("TinyMQ Ch32SnSensor");
#ifdef LED_BUILTIN
    pinMode(LED_BUILTIN, OUTPUT);
#endif
    if (!ETH.begin()) {
        Serial.println("ETH.begin() failed");
        return;
    }
    while (!ETH.hasIP())
        delay(100);
    Serial.print("Local IP: ");
    Serial.println(ETH.localIP());

    udp.begin(0);
    sn.setGateway(kGateway, kGatewayPort);
    sn.setClientId("ch32-sensor");
    sn.setKeepAlive(60);
    sn.onMessage(onMessage);
    connectAndSubscribe();
}

void loop()
{
    sn.update();
    static uint32_t last = 0;
    if (millis() - last >= 10000) {
        last = millis();
        char text[16];
        snprintf(text, sizeof(text), "%lu", (unsigned long)(millis() / 1000));
        if (sn.connected())
            sn.publish("sensors/uptime", text, 1);   // en: registered on first use / ja: 初回に登録される
        else
            sn.publishPredefined(1, text);           // en: QoS -1: no connection needed / ja: QoS -1 は接続なしで送れる
    }
    if (!sn.connected() && millis() % 30000 < 10)
        connectAndSubscribe();
}
