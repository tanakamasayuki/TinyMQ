// Generated from examples/Esp32Bridge/Esp32Bridge.ino by tests/tools/sync_examples.py:
// do not edit. Settings come from tests/.env (build_config.toml).
// en: A local broker that forwards "sensors/#" to an upstream broker as "site1/sensors/#",
//     and "site1/cmd/#" from upstream to local clients as "cmd/#".
// ja: ローカルのブローカーが "sensors/#" を上流のブローカーへ "site1/sensors/#" として送り、
//     上流の "site1/cmd/#" をローカルのクライアントへ "cmd/#" として届ける。
#include <WiFi.h>
#include <TinyMQ.h>

static const char *kWifiSsid = WIFI_SSID;
static const char *kWifiPass = WIFI_PASSWORD;

// en: The upstream broker (in the cloud or on the LAN).
// ja: 上流のブローカー(クラウドや LAN の上)。
static const char *kUpstreamHost = HOST_IP;
static constexpr uint16_t kUpstreamPort = 1883;

TinyMQBroker broker;
TinyMQLwipListener tcp(1883);
WiFiClient upstreamNet;  // en: WiFiClientSecure for TLS / ja: TLS なら WiFiClientSecure
TinyMQBridge bridge(upstreamNet);

void setup()
{
    Serial.begin(115200);
    delay(500);
    Serial.println("TinyMQ Esp32Bridge");

    WiFi.mode(WIFI_STA);
    WiFi.setSleep(false);
    WiFi.begin(kWifiSsid, kWifiPass);
    while (WiFi.status() != WL_CONNECTED) {
        delay(200);
        Serial.print('.');
    }
    Serial.print("\nBroker at ");
    Serial.println(WiFi.localIP());

    bridge.client().setServer(kUpstreamHost, kUpstreamPort);
    bridge.client().setClientId("site1-bridge");
    bridge.client().setProtocolVersion(tinymq::kMqtt5);  // en: No Local keeps "both" topics from looping / ja: No Local で both のトピックがループしない
    bridge.addTopic("sensors/#", TinyMQBridge::kOut, 1, "", "site1/");
    bridge.addTopic("cmd/#", TinyMQBridge::kIn, 1, "", "site1/");

    broker.begin();
    tcp.begin();
    broker.addListener(tcp);
    broker.addListener(bridge);
}

void loop()
{
    broker.update();
    static uint32_t last = 0;
    if (millis() - last >= 10000) {
        last = millis();
        Serial.printf("upstream %s, out %lu, in %lu, dropped %lu\n", bridge.connected() ? "up" : "down", (unsigned long)bridge.forwardedOut(),
                      (unsigned long)bridge.forwardedIn(), (unsigned long)bridge.dropped());
    }
}
