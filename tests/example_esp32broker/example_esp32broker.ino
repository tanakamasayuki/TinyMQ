// Generated from examples/Esp32Broker/Esp32Broker.ino by tests/tools/sync_examples.py:
// do not edit. Settings come from tests/.env (build_config.toml).
// en: An MQTT broker on an ESP32: MQTT over TCP (port 1883) and MQTT-SN over UDP (port 1884),
//     and the sketch itself subscribed to "sensors/#".
// ja: ESP32 で動く MQTT ブローカー。TCP の MQTT(1883 番)と UDP の MQTT-SN(1884 番)を受け付け、
//     スケッチ自身も "sensors/#" を購読する。
#include <WiFi.h>
#include <WiFiUdp.h>
#include <TinyMQ.h>

static const char *kWifiSsid = WIFI_SSID;
static const char *kWifiPass = WIFI_PASSWORD;

TinyMQBroker broker;
TinyMQLwipListener tcp(1883);  // en: MQTT 3.1.1 / 5.0 / ja: MQTT 3.1.1 / 5.0
WiFiUDP udp;
TinyMQSnListener sn(udp);      // en: MQTT-SN 1.2 gateway / ja: MQTT-SN 1.2 のゲートウェイ

static void onMessage(const tinymq::Message &m, void *)
{
    Serial.print(m.topic);
    Serial.print(" = ");
    Serial.write(m.payload, m.payloadLen);
    Serial.println();
}

static void onClient(tinymq::Bytes id, bool connected, void *)
{
    Serial.print(connected ? "client in:  " : "client out: ");
    Serial.write(id.data, id.len);
    Serial.println();
}

void setup()
{
    Serial.begin(115200);
    delay(500);
    Serial.println("TinyMQ Esp32Broker");

    WiFi.mode(WIFI_STA);
    WiFi.setSleep(false);  // en: power save delays incoming packets / ja: 省電力は受信を遅らせる
    WiFi.begin(kWifiSsid, kWifiPass);
    while (WiFi.status() != WL_CONNECTED) {
        delay(200);
        Serial.print('.');
    }
    Serial.print("\nBroker at ");
    Serial.println(WiFi.localIP());

    broker.begin();
    tcp.begin();
    udp.begin(1884);
    // en: MQTT-SN clients can publish "sensors/temperature" as topic id 1 without connecting (QoS -1).
    // ja: MQTT-SN のクライアントは、接続しなくても topic id 1 で "sensors/temperature" に publish できる(QoS -1)。
    sn.addPredefinedTopic(1, "sensors/temperature");
    broker.addListener(tcp);
    broker.addListener(sn);
    broker.onClient(onClient);
    broker.onMessage(onMessage);
    broker.subscribe("sensors/#");
}

void loop()
{
    broker.update();
}
