"""examples/Esp32Broker on a board: a TCP client and an MQTT-SN QoS -1
publish reach the sketch's own subscription to "sensors/#"."""

from helpers import sn_peer as snp
from helpers.example_check import assert_copy_current, connect_retry, reset_esp32
from helpers.line_dut import LineDut
from helpers.paho_peer import PahoPeer


def test_example_esp32broker(dut):
    assert_copy_current()
    d = LineDut(dut)
    reset_esp32(d)
    ip = d.expect(r"^Broker at (\S+)", 60).group(1)
    pc = connect_retry(lambda: PahoPeer(1883, client_id="pc-example", host=ip))
    try:
        d.expect(r"^client in:  pc-example$")
        pc.publish("sensors/room", "22.5", qos=1)
        d.expect(r"^sensors/room = 22\.5$")
        # QoS -1 to the predefined topic 1 = "sensors/temperature", no connection
        sn = snp.SnPeer(1884, host=ip)
        try:
            sn.publish(1, "19.5", qos=-1, topic_type=snp.PREDEFINED)
            d.expect(r"^sensors/temperature = 19\.5$")
        finally:
            sn.close()
    finally:
        pc.close()
    d.expect(r"^client out: pc-example$")
