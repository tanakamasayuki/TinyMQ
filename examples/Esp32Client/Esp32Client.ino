// en: Connect to an MQTT broker, publish a counter every 5 s and print what arrives on "tinymq/cmd".
// ja: MQTT ブローカーに接続し、5 秒ごとにカウンタを publish し、"tinymq/cmd" に届いたものを表示する。
#include <WiFi.h>
#include <TinyMQ.h>

static const char *kWifiSsid = "your-ssid";
static const char *kWifiPass = "your-passphrase";

// en: The broker (mosquitto on a PC, another board running Esp32Broker, ...).
// ja: ブローカー(PC の mosquitto、Esp32Broker を動かす別のボードなど)。
static const IPAddress kBroker(192, 168, 1, 10);
static constexpr uint16_t kBrokerPort = 1883;

WiFiClient net;
TinyMQClient mqtt(net);
uint32_t lastPublish = 0;
uint32_t counter = 0;

static void onMessage(const tinymq::Message &m, void *)
{
    Serial.print("received ");
    Serial.print(m.topic);
    Serial.print(": ");
    Serial.write(m.payload, m.payloadLen);
    Serial.println();
}

void setup()
{
    Serial.begin(115200);
    delay(500);
    Serial.println("TinyMQ Esp32Client");

    WiFi.mode(WIFI_STA);
    WiFi.begin(kWifiSsid, kWifiPass);
    while (WiFi.status() != WL_CONNECTED) {
        delay(200);
        Serial.print('.');
    }
    Serial.print("\nLocal IP: ");
    Serial.println(WiFi.localIP());

    mqtt.setServer(kBroker, kBrokerPort);
    mqtt.setClientId("esp32-client");
    mqtt.setWill("tinymq/status", "offline", 1, true);  // en: published if we vanish / ja: 黙って消えたときに出る
    mqtt.setReconnectInterval(5000);                    // en: keep trying / ja: 切れたら 5 秒ごとにつなぎ直す
    mqtt.onMessage(onMessage);
    if (mqtt.connect()) {
        Serial.println("connected");
        mqtt.publish("tinymq/status", "online", 1, true);
        mqtt.subscribe("tinymq/cmd", 1);
    } else {
        Serial.print("connect failed, reason 0x");
        Serial.println(mqtt.lastReason(), HEX);
    }
}

void loop()
{
    mqtt.update();
    if (mqtt.connected() && millis() - lastPublish >= 5000) {
        lastPublish = millis();
        char text[16];
        snprintf(text, sizeof(text), "%lu", (unsigned long)counter++);
        mqtt.publish("tinymq/counter", text, 1);
    }
}
