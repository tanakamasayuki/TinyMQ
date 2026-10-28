// PASS / FAIL lines and the closing "TEST done N/M" read by test_*.py.
#pragma once

#include <Arduino.h>

static int g_pass = 0;
static int g_total = 0;

static void report(const char *name, bool ok, const char *what)
{
    ++g_total;
    if (ok) {
        ++g_pass;
        Serial.print("PASS ");
        Serial.println(name);
    } else {
        Serial.print("FAIL ");
        Serial.print(name);
        Serial.print(" ");
        Serial.println(what);
    }
}

#define EXPECT_TRUE(name, cond) report(name, (cond), #cond)
#define EXPECT_EQ(name, a, b) report(name, (a) == (b), #a " == " #b)

static void printHex(const uint8_t *p, size_t n)
{
    for (size_t i = 0; i < n; ++i) {
        if (p[i] < 16)
            Serial.print('0');
        Serial.print(p[i], HEX);
        Serial.print(' ');
    }
    Serial.println();
}

// Compares bytes and prints both sides on a mismatch
static void expectBytes(const char *name, const uint8_t *got, size_t gotLen, const uint8_t *want, size_t wantLen)
{
    bool ok = gotLen == wantLen && memcmp(got, want, wantLen) == 0;
    report(name, ok, "bytes differ");
    if (!ok) {
        Serial.print("  got  ");
        printHex(got, gotLen);
        Serial.print("  want ");
        printHex(want, wantLen);
    }
}

static void finishTests()
{
    Serial.print("TEST done ");
    Serial.print(g_pass);
    Serial.print("/");
    Serial.println(g_total);
}
