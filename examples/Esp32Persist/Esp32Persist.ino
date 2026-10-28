// en: A broker that keeps retained messages and persistent sessions across restarts in LittleFS.
// ja: retain メッセージと永続セッションを LittleFS に保存し、再起動しても引き継ぐブローカー。
#include <WiFi.h>
#include <LittleFS.h>
#include <TinyMQ.h>

static const char *kWifiSsid = "your-ssid";
static const char *kWifiPass = "your-passphrase";
static const char *kStateFile = "/tinymq.state";

TinyMQBroker broker;
TinyMQLwipListener tcp(1883);

// en: saveState() / loadState() go through these, to and from a file.
// ja: saveState() / loadState() はこれを通してファイルに読み書きする。
struct FileSink : tinymq::StateSink {
    File f;
    bool write(const uint8_t *data, size_t len) override { return f.write(data, len) == len; }
};
struct FileSource : tinymq::StateSource {
    File f;
    bool read(uint8_t *data, size_t len) override { return f.read(data, len) == (int)len; }
};

static bool dirty = false;
static uint32_t dirtySince = 0;

static void onRetained(tinymq::Bytes, bool, void *)
{
    if (!dirty)
        dirtySince = millis();
    dirty = true;
}

static void save()
{
    // en: write a new file, then replace the old one, so a reset meanwhile leaves the old state
    // ja: 新しいファイルに書いてから入れ替える。途中でリセットされても古い状態が残る
    FileSink sink;
    sink.f = LittleFS.open("/tinymq.new", "w");
    bool ok = sink.f && broker.saveState(sink);
    sink.f.close();
    if (ok) {
        LittleFS.remove(kStateFile);
        LittleFS.rename("/tinymq.new", kStateFile);
    }
    Serial.println(ok ? "state saved" : "state not saved");
}

void setup()
{
    Serial.begin(115200);
    delay(500);
    Serial.println("TinyMQ Esp32Persist");
    LittleFS.begin(true);  // en: formats on first use / ja: 初回はフォーマットする

    WiFi.mode(WIFI_STA);
    WiFi.setSleep(false);
    WiFi.begin(kWifiSsid, kWifiPass);
    while (WiFi.status() != WL_CONNECTED) {
        delay(200);
        Serial.print('.');
    }
    Serial.print("\nBroker at ");
    Serial.println(WiFi.localIP());

    broker.begin();
    FileSource source;
    source.f = LittleFS.open(kStateFile, "r");
    if (source.f) {
        Serial.println(broker.loadState(source) ? "state loaded" : "state file unreadable");
        source.f.close();
    }
    broker.onRetainedChange(onRetained);
    tcp.begin();
    broker.addListener(tcp);
}

void loop()
{
    broker.update();
    // en: save 2 s after the retained messages changed, and every 5 minutes for the sessions
    // ja: retain が変わってから 2 秒後と、セッションのために 5 分ごとに保存する
    static uint32_t lastSave = 0;
    if ((dirty && millis() - dirtySince >= 2000) || millis() - lastSave >= 300000) {
        dirty = false;
        lastSave = millis();
        save();
    }
}
