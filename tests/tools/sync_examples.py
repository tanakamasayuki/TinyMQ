#!/usr/bin/env python3
"""Copies the examples into tests/example_<name>/ for the board tests.

Each copy is the example verbatim except its settings, which become macros
fed from tests/.env through build_config.toml:

    "your-ssid" / "your-passphrase"  ->  WIFI_SSID / WIFI_PASSWORD  (TEST_WIFI_SSID / TEST_WIFI_PASSWORD)
    the broker, upstream or gateway  ->  HOST_IP                    (TEST_HOST_IP: the PC)

    python3 tests/tools/sync_examples.py           # write the copies
    python3 tests/tools/sync_examples.py --check   # exit 1 if a copy is out of date

The tests run --check first (expected_sketch()), so an example and its test
cannot drift apart.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TESTS = ROOT / "tests"

WIFI = [
    ('static const char *kWifiSsid = "your-ssid";', "static const char *kWifiSsid = WIFI_SSID;"),
    ('static const char *kWifiPass = "your-passphrase";', "static const char *kWifiPass = WIFI_PASSWORD;"),
]

# example -> (profiles, substitutions)
EXAMPLES = {
    "Esp32Broker": (["esp32", "esp32s3"], WIFI),
    "Esp32Client": (["esp32", "esp32s3"], WIFI + [
        ("static const IPAddress kBroker(192, 168, 1, 10);", "static const IPAddress kBroker(HOST_IP);"),
    ]),
    "Esp32Persist": (["esp32", "esp32s3"], WIFI),
    "Esp32Bridge": (["esp32", "esp32s3"], WIFI + [
        ('static const char *kUpstreamHost = "broker.example.com";', "static const char *kUpstreamHost = HOST_IP;"),
    ]),
    "Ch32SnSensor": (["ch32v307"], [
        ("static const IPAddress kGateway(192, 168, 1, 20);", "static const IPAddress kGateway(HOST_IP);"),
    ]),
}

ESP32_PLATFORM = """    platforms:
      - platform: esp32:esp32 (3.3.12)
        platform_index_url: https://espressif.github.io/arduino-esp32/package_esp32_index.json
"""
PROFILES = {
    "esp32": "    fqbn: esp32:esp32:esp32\n" + ESP32_PLATFORM,
    "esp32s3": "    fqbn: esp32:esp32:esp32s3\n" + ESP32_PLATFORM,
    "ch32v307": """    fqbn: ch32-riscv-ug:ch32rv:CH32V307:pnum=ANY
    platforms:
      - platform: ch32-riscv-ug:ch32rv (0.4.0)
        platform_index_url: https://ch32-riscv-ug.github.io/ArduinoCore-CH32RV/package_ch32-riscv-ug_index.json
""",
}

BUILD_CONFIG = """[defines]
TEST_WIFI_SSID = "WIFI_SSID"
TEST_WIFI_PASSWORD = "WIFI_PASSWORD"
TEST_HOST_IP = "HOST_IP"
"""


def test_dir(name):
    return TESTS / f"example_{name.lower()}"


def expected_files(name):
    profiles, subs = EXAMPLES[name]
    src = (ROOT / "examples" / name / f"{name}.ino").read_text()
    for old, new in subs:
        if src.count(old) != 1:
            raise SystemExit(f"examples/{name}: expected one line {old!r}")
        src = src.replace(old, new)
    header = (f"// Generated from examples/{name}/{name}.ino by tests/tools/sync_examples.py:\n"
              "// do not edit. Settings come from tests/.env (build_config.toml).\n")
    sketch = "profiles:\n" + "".join(
        f"  {p}:\n{PROFILES[p]}    libraries:\n      - dir: ../../\n\n" for p in profiles
    ) + f"default_profile: {profiles[0]}\n"
    d = test_dir(name)
    return {
        d / f"{d.name}.ino": header + src,
        d / "sketch.yaml": sketch,
        d / "build_config.toml": BUILD_CONFIG,
    }


def stale(name):
    """Files of the example's test copy that differ from what this script writes."""
    return [p for p, text in expected_files(name).items() if not p.exists() or p.read_text() != text]


def main():
    check = "--check" in sys.argv[1:]
    bad = []
    for name in EXAMPLES:
        for path, text in expected_files(name).items():
            if path.exists() and path.read_text() == text:
                continue
            if check:
                bad.append(path.relative_to(ROOT))
            else:
                path.parent.mkdir(exist_ok=True)
                path.write_text(text)
                print(f"wrote {path.relative_to(ROOT)}")
    if bad:
        print("out of date (run tests/tools/sync_examples.py):", *bad, sep="\n  ")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
