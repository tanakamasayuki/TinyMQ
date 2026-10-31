"""examples/Ch32SnSensor on a CH32V307 over Ethernet, with a scripted
MQTT-SN gateway on the PC (TEST_HOST_IP, UDP 1884): it connects, subscribes
to "cmd/led", publishes "sensors/uptime" and switches the LED on "on"."""

import socket
import struct
import time

from helpers import sn_peer as snp
from helpers.example_check import assert_copy_current
from helpers.line_dut import LineDut


def test_example_ch32snsensor(dut):
    assert_copy_current()
    d = LineDut(dut)
    gw = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    gw.bind(("0.0.0.0", 1884))
    try:
        led_tid, sensor_tid, uptime = 0x0101, 0x0202, None
        peer = None

        def send(t, body=b""):
            gw.sendto(snp.frame(t, body), peer)

        deadline = time.time() + 90
        while uptime is None:
            assert time.time() < deadline, "no PUBLISH of sensors/uptime"
            gw.settimeout(max(0.1, deadline - time.time()))
            data, peer = gw.recvfrom(1024)
            t, body = data[1], data[2:]
            if t == snp.CONNECT:
                assert body[4:] == b"ch32-sensor", body
                send(snp.CONNACK, b"\x00")
            elif t == snp.SUBSCRIBE:
                mid = struct.unpack(">H", body[1:3])[0]
                assert body[3:] == b"cmd/led", body
                send(snp.SUBACK, bytes([body[0] & 0x60]) + struct.pack(">HHB", led_tid, mid, 0))
            elif t == snp.REGISTER:
                mid = struct.unpack(">H", body[2:4])[0]
                assert body[4:] == b"sensors/uptime", body
                send(snp.REGACK, struct.pack(">HHB", sensor_tid, mid, 0))
            elif t == snp.PUBLISH:
                tid, mid = struct.unpack(">HH", body[1:5])
                if snp.parse_flags(body[0])["qos"] == 1:
                    send(snp.PUBACK, struct.pack(">HHB", tid, mid, 0))
                if tid == sensor_tid:
                    uptime = int(body[5:])
            elif t == snp.PINGREQ:
                send(snp.PINGRESP)
        d.expect(r"^connected$", 5)
        send(snp.PUBLISH, bytes([snp.flags(qos=0)]) + struct.pack(">HH", led_tid, 0) + b"on")
        d.expect(r"^led on$", 10)
        send(snp.PUBLISH, bytes([snp.flags(qos=0)]) + struct.pack(">HH", led_tid, 0) + b"off")
        d.expect(r"^led off$", 10)
    finally:
        gw.close()
