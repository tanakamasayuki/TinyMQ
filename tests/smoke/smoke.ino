// Smoke test: the library builds for the profile and the harness sees the DUT.

#include <TinyMQ.h>

void setup()
{
    Serial.begin(115200);
    delay(5000); // give the real ESP32 Serial-USB bridge time to settle
    Serial.print("TinyMQ ");
    Serial.println(TINYMQ_VERSION_STR);
    Serial.println("SMOKE ready");
}

void loop()
{
    delay(1);
}
