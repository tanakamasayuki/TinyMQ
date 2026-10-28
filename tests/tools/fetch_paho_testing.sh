#!/bin/sh
# Fetches the Eclipse paho.mqtt.testing interoperability suites into
# tests/.tools/paho.mqtt.testing for broker_conformance/. A test tool only;
# it is not part of the library.
set -e
cd "$(dirname "$0")/.."
mkdir -p .tools
if [ -d .tools/paho.mqtt.testing ]; then
    git -C .tools/paho.mqtt.testing pull -q
else
    git clone -q --depth 1 https://github.com/eclipse-paho/paho.mqtt.testing.git .tools/paho.mqtt.testing
fi
echo "paho.mqtt.testing in $PWD/.tools/paho.mqtt.testing"
