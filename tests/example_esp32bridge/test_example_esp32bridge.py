"""examples/Esp32Bridge on a board, with mosquitto on the PC as the upstream
(TEST_HOST_IP, port 1883): "sensors/#" goes up as "site1/sensors/#" and
"site1/cmd/#" comes down as "cmd/#"."""

import time

from helpers.example_check import assert_copy_current, connect_retry, reset_esp32
from helpers.line_dut import LineDut
from helpers.mosquitto import mosquitto
from helpers.paho_peer import PahoPeer


def test_example_esp32bridge(dut, tmp_path):
    assert_copy_current()
    d = LineDut(dut)
    reset_esp32(d)
    with mosquitto(tmp_path, bind="0.0.0.0", port=1883) as (port, _):
        ip = d.expect(r"^Broker at (\S+)", 60).group(1)
        d.expect(r"^upstream up", 40)
        up = PahoPeer(port, client_id="pc-upstream", host="127.0.0.1")
        local = connect_retry(lambda: PahoPeer(1883, client_id="pc-local", host=ip))
        try:
            up.subscribe("site1/sensors/#", 1)
            local.subscribe("cmd/#", 1)
            time.sleep(0.5)
            local.publish("sensors/t", "21", qos=1)
            m = up.expect(timeout=10)
            assert (m.topic, m.payload) == ("site1/sensors/t", b"21")
            up.publish("site1/cmd/led", "on", qos=1)
            m = local.expect(timeout=10)
            assert (m.topic, m.payload) == ("cmd/led", b"on")
        finally:
            local.close()
            up.close()
