// Network for the interop sketches: Wi-Fi on ESP32 and Pico W (SSID and
// password from tests/.env through build_config.toml), nothing on the host
// core, whose sockets use the PC's network.
#pragma once

#include <WiFi.h>

#ifndef WIFI_SSID
#define WIFI_SSID ""
#define WIFI_PASSWORD ""
#endif

#if defined(ARDUINO_ARCH_ESP32) || defined(ARDUINO_ARCH_RP2040)
#define BOARD_NET_WIFI 1
#endif

// Joins the network; prints NET_ERROR and returns false if it cannot.
static bool networkUp()
{
#ifdef BOARD_NET_WIFI
    if (strlen(WIFI_SSID) == 0) {
        Serial.println("NET_ERROR missing_ssid (set TEST_WIFI_SSID in tests/.env)");
        return false;
    }
    WiFi.mode(WIFI_STA);
#ifdef ARDUINO_ARCH_ESP32
    WiFi.setSleep(false);  // power save delays incoming packets by up to a beacon interval
#endif
    WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
    for (uint32_t start = millis(); WiFi.status() != WL_CONNECTED && millis() - start < 120000;)
        delay(250);
    if (WiFi.status() != WL_CONNECTED) {
        Serial.print("NET_ERROR wifi_connect_failed status=");
        Serial.println((int)WiFi.status());
        return false;
    }
#endif
    return true;
}

// The address the PC reaches the sketch at
static IPAddress networkIP()
{
#ifdef BOARD_NET_WIFI
    return WiFi.localIP();
#else
    return IPAddress(127, 0, 0, 1);
#endif
}

// Startup: give a USB serial bridge time to settle, join the network and
// print "READY <ip>" (the test waits for it).
static bool networkReady()
{
#ifdef BOARD_NET_WIFI
    delay(5000);
#else
    delay(2000);
#endif
    if (!networkUp())
        return false;
    Serial.print("READY ");
    Serial.println(networkIP());
    return true;
}
