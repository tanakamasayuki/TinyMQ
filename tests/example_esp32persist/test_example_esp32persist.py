"""examples/Esp32Persist on a board: a retained message is saved to
LittleFS and is there again after a reset."""

import time

from helpers.example_check import assert_copy_current, connect_retry, reset_esp32
from helpers.line_dut import LineDut
from helpers.paho_peer import PahoPeer


def test_example_esp32persist(dut):
    assert_copy_current()
    d = LineDut(dut)
    reset_esp32(d)
    d.expect(r"TinyMQ Esp32Persist", 20)
    # the first boot formats LittleFS, which takes a while
    ip = d.expect(r"Broker at (\S+)", 240).group(1)
    value = f"kept-{int(time.time())}"
    pc = connect_retry(lambda: PahoPeer(1883, client_id="pc-persist", host=ip))
    try:
        pc.publish("persist/x", value, qos=1, retain=True)
    finally:
        pc.close()
    d.expect(r"^state saved$", 10)
    reset_esp32(d)
    d.expect(r"^TinyMQ Esp32Persist$", 20)
    ip = d.expect(r"Broker at (\S+)", 120).group(1)
    d.expect(r"^state loaded$", 10)
    pc = connect_retry(lambda: PahoPeer(1883, client_id="pc-persist", host=ip))
    try:
        pc.subscribe("persist/x", 1)
        m = pc.expect(timeout=10)
        assert m.retain and m.payload == value.encode(), (m.topic, m.payload)
        pc.publish("persist/x", "", qos=1, retain=True)  # leave nothing behind
    finally:
        pc.close()
    d.expect(r"^state saved$", 10)
