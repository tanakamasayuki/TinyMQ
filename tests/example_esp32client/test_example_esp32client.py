"""examples/Esp32Client on a board, against mosquitto on the PC (TEST_HOST_IP,
port 1883): the counter arrives, and "tinymq/cmd" is printed."""

from helpers.example_check import assert_copy_current, connect_retry, reset_esp32
from helpers.line_dut import LineDut
from helpers.mosquitto import mosquitto
from helpers.paho_peer import PahoPeer


def test_example_esp32client(dut, tmp_path):
    assert_copy_current()
    d = LineDut(dut)
    reset_esp32(d)
    with mosquitto(tmp_path, bind="0.0.0.0", port=1883) as (port, _):
        d.expect(r"^Local IP: ", 60)
        pc = PahoPeer(port, client_id="pc-example", host="127.0.0.1")
        try:
            pc.subscribe("tinymq/#", 1)
            # connected at boot, or by the 5 s reconnect if mosquitto came up later
            m = pc.expect(timeout=30)
            while m.topic != "tinymq/counter":
                m = pc.expect(timeout=30)
            int(m.payload)
            pc.publish("tinymq/cmd", "hello", qos=1)
            d.expect(r"^received tinymq/cmd: hello$")
        finally:
            pc.close()
