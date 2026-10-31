// Network for the interop sketches: Ethernet on ESP32-P4 (the PHY from the
// board variant, address by DHCP), Wi-Fi on the other ESP32 and Pico W (SSID
// and password from tests/.env through build_config.toml), nothing on the
// host core, whose sockets use the PC's network.
#pragma once

#include <WiFi.h>
#if defined(CONFIG_IDF_TARGET_ESP32P4)
#include <ETH.h>
#endif

#ifndef WIFI_SSID
#define WIFI_SSID ""
#define WIFI_PASSWORD ""
#endif

#if defined(CONFIG_IDF_TARGET_ESP32P4)
#define BOARD_NET_ETH 1
#elif defined(ARDUINO_ARCH_ESP32) || defined(ARDUINO_ARCH_RP2040)
#define BOARD_NET_WIFI 1
#endif

// Joins the network; prints NET_ERROR and returns false if it cannot.
static bool networkUp()
{
#ifdef BOARD_NET_ETH
    if (!ETH.begin()) {
        Serial.println("NET_ERROR eth_begin");
        return false;
    }
    for (uint32_t start = millis(); !ETH.hasIP() && millis() - start < 30000;)
        delay(100);
    if (!ETH.hasIP()) {
        Serial.print("NET_ERROR eth_no_ip link=");
        Serial.println(ETH.linkUp() ? 1 : 0);
        return false;
    }
#endif
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
#if defined(BOARD_NET_ETH)
    return ETH.localIP();
#elif defined(BOARD_NET_WIFI)
    return WiFi.localIP();
#else
    return IPAddress(127, 0, 0, 1);
#endif
}

// Startup: give a USB serial bridge time to settle, join the network and
// print "READY <ip>" (the test waits for it).
static bool networkReady()
{
#if defined(BOARD_NET_WIFI) || defined(BOARD_NET_ETH)
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
