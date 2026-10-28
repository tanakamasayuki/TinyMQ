"""TinyMQClient (MQTT 3.1.1 / 3.1 / 5.0) against mosquitto and a scripted fake broker
(5.0 in client_v5_scenarios.py).

The host DUT accepts one serial connection per process, so the scenarios
run inside one test function and are reported one by one
(SCENARIO PASS/FAIL and junit properties).
"""

import re
import time
from pathlib import Path

import paho.mqtt.client as mqtt

from helpers import fake_broker as fb
from helpers.line_dut import LineDut
from helpers.mosquitto import make_password_file, mosquitto
from helpers.paho_peer import PahoPeer
from helpers import target
from client_v5_scenarios import SCENARIOS as V5_SCENARIOS

OUTPUT = Path(__file__).parent / "output"


class Dut(LineDut):
    """Serial commands of client_interop.ino."""

    def connect(self, ok=True, timeout=15):
        """Returns (reason, session present)."""
        self.send("CONNECT")
        m = self.expect(r"CONNECT (ok|fail) ([0-9A-F]{2})", timeout)
        result = m.group(1)
        assert (result == "ok") == ok, f"CONNECT {result} reason 0x{m.group(2)}"
        if not ok:
            return int(m.group(2), 16), False
        c = self.expect(r"CONNACK ([0-9A-F]{2}) ([01])", 1)
        return int(c.group(1), 16), c.group(2) == "1"

    def subscribe(self, filter_, qos):
        self.send(f"SUB {qos} {filter_}")
        pid = int(self.expect(r"^SUB (-?\d+)").group(1))
        assert pid > 0
        m = self.expect(rf"^SUBACK {pid} ([0-9A-F ]+)$")
        return [int(c, 16) for c in m.group(1).split()]

    def publish(self, qos, topic, payload, retain=False):
        self.send(f"PUB {qos} {1 if retain else 0} {topic} {payload}")
        pid = int(self.expect(r"^PUB (-?\d+)").group(1))
        assert pid >= 0
        return pid

    def message(self, timeout=10):
        m = self.expect(r"^MSG (\d) (\d) (\d) (\S+) (\d+) ?(.*)$", timeout)
        return {
            "qos": int(m.group(1)),
            "retain": m.group(2) == "1",
            "dup": m.group(3) == "1",
            "topic": m.group(4),
            "len": int(m.group(5)),
            "payload": m.group(6),
        }

    def no_message(self, wait=1.0):
        self.expect_none(r"^MSG .*", wait)

    def status(self):
        self.send("STATUS")
        m = self.expect(r"^STATUS connected=(\d) inflight=(\d+) dropped=(\d+)")
        return {"connected": m.group(1) == "1", "inflight": int(m.group(2)), "dropped": int(m.group(3))}

    def disconnect(self):
        self.send("DISCONNECT")
        self.expect(r"^CLOSED 00")

    def reset(self, port, client_id="tinymq", clean=True, keepalive=60, proto=4):
        self.backlog.clear()
        self.send(f"SERVER {self.server} {port}")
        self.send(f"ID {client_id}")
        self.send(f"CLEAN {1 if clean else 0}")
        self.send(f"KEEPALIVE {keepalive}")
        self.send(f"PROTO {proto}")
        self.send("NOWILL")
        self.send("NOAUTH")


# ---- scenarios against mosquitto


def scenario_pubsub(d, port):
    """QoS 0/1/2 both ways, with the QoS downgraded to the subscription's."""
    pc = PahoPeer(port)
    try:
        d.reset(port)
        d.connect()
        assert d.subscribe("tm/in/#", 2) == [2]
        assert d.subscribe("tm/low", 1) == [1]
        for qos in (0, 1, 2):
            pc.publish("tm/in/a", f"to-dut-{qos}", qos=qos)
            m = d.message()
            assert (m["topic"], m["payload"], m["qos"], m["retain"]) == ("tm/in/a", f"to-dut-{qos}", qos, False), m
        pc.publish("tm/low", "capped", qos=2)
        m = d.message()
        assert m["qos"] == 1 and m["payload"] == "capped", m

        pc.subscribe("tm/out", 2)
        for qos in (0, 1, 2):
            pid = d.publish(qos, "tm/out", f"from-dut-{qos}")
            if qos:
                d.expect(rf"PUBDONE {pid} 00")
            m = pc.expect()
            assert (m.topic, m.payload, m.qos) == ("tm/out", f"from-dut-{qos}".encode(), qos)
        assert d.status()["inflight"] == 0
        d.disconnect()
    finally:
        pc.close()


def scenario_retain_unsubscribe(d, port):
    pc = PahoPeer(port)
    try:
        pc.publish("tm/ret", "kept", qos=1, retain=True)
        d.reset(port)
        d.connect()
        d.subscribe("tm/ret", 1)
        m = d.message()
        assert (m["payload"], m["retain"], m["qos"]) == ("kept", True, 1), m
        # a retained message published by the DUT reaches a later subscriber
        pid = d.publish(1, "tm/ret2", "from-dut", retain=True)
        d.expect(rf"PUBDONE {pid} 00")
        pc.subscribe("tm/ret2", 0)
        m2 = pc.expect()
        assert m2.retain and m2.payload == b"from-dut"

        d.send("UNSUB tm/ret")
        pid = int(d.expect(r"UNSUB (\d+)").group(1))
        d.expect(rf"UNSUBACK {pid}")
        pc.publish("tm/ret", "after-unsub", qos=1)
        d.no_message()
        pc.publish("tm/ret", "", qos=1, retain=True)  # clear
        pc.publish("tm/ret2", "", qos=1, retain=True)
        d.disconnect()
    finally:
        pc.close()


def scenario_will(d, port):
    pc = PahoPeer(port)
    try:
        pc.subscribe("tm/will", 1)
        d.reset(port)
        d.send("WILL 1 0 tm/will gone away")
        d.connect()
        d.disconnect()  # a normal DISCONNECT discards the will
        pc.expect_none(1.0)
        d.connect()
        d.send("DROP")  # the socket closes without DISCONNECT
        m = pc.expect()
        assert (m.topic, m.payload, m.qos) == ("tm/will", b"gone away", 1)
        d.expect(r"CLOSED")
    finally:
        pc.close()


def scenario_persistent(d, port):
    """Messages queued by the broker while the DUT was away arrive on reconnect."""
    pc = PahoPeer(port)
    try:
        d.reset(port, client_id="tm-persist", clean=False)
        assert d.connect() == (0, False)
        d.subscribe("tm/p", 1)
        d.disconnect()
        pc.publish("tm/p", "while-away-1", qos=1)
        pc.publish("tm/p", "while-away-2", qos=1)
        assert d.connect() == (0, True)
        assert d.message()["payload"] == "while-away-1"
        assert d.message()["payload"] == "while-away-2"
        d.disconnect()
        d.send("CLEAN 1")  # leave no session behind
        assert d.connect() == (0, False)
        d.disconnect()
    finally:
        pc.close()


def scenario_keepalive(d, port, log_path):
    d.reset(port, client_id="tm-ka", keepalive=1)
    d.connect()
    time.sleep(3.5)
    assert d.status()["connected"]
    d.disconnect()
    pings = sum(1 for line in open(log_path) if "PINGREQ from tm-ka" in line)
    assert pings >= 2, f"{pings} PINGREQ in the broker log"


def scenario_oversize(d, port):
    """A message larger than the receive buffer is dropped (and acknowledged); the connection stays."""
    pc = PahoPeer(port)
    try:
        d.reset(port)
        d.connect()
        d.subscribe("tm/big", 1)
        before = d.status()["dropped"]
        pc.publish("tm/big", b"x" * 3000, qos=1)
        pc.publish("tm/big", "small", qos=1)
        m = d.message()
        assert m["payload"] == "small", m
        s = d.status()
        assert s["connected"] and s["dropped"] == before + 1, s
        d.disconnect()
    finally:
        pc.close()


def scenario_mqtt31(d, port):
    d.reset(port, proto=3)
    d.connect()
    pc = PahoPeer(port)
    try:
        d.subscribe("tm/31", 1)
        pc.publish("tm/31", "old", qos=1)
        assert d.message()["payload"] == "old"
        d.disconnect()
    finally:
        pc.close()


def scenario_auth(d):
    pw = OUTPUT / "passwd"
    make_password_file(pw, {"alice": "secret"})
    with mosquitto(OUTPUT, extra_conf=f"password_file {pw}", allow_anonymous=False, bind=d.bind) as (port, _):
        d.reset(port)
        reason, _ = d.connect(ok=False)
        assert reason in (0x86, 0x87), hex(reason)
        d.send("AUTH alice wrong")
        reason, _ = d.connect(ok=False)
        assert reason in (0x86, 0x87), hex(reason)
        d.send("AUTH alice secret")
        d.connect()
        d.disconnect()


# ---- scenarios against the fake broker


def scenario_resend(d):
    """Unacknowledged QoS 1 / 2 messages are resent after reconnecting with a session."""
    fake = fb.FakeBroker(d.bind)
    try:
        d.reset(fake.port, client_id="tm-resend", clean=False)
        d.send("CONNECT")
        fake.accept()
        first, _ = fake.recv_packet()
        assert first == 0x10
        fake.send(fb.connack(False))
        d.expect(r"CONNECT ok")

        # QoS 1 not acknowledged, QoS 2 acknowledged up to PUBREC
        pid1 = d.publish(1, "tm/r1", "one")
        first, body = fake.recv_packet()
        topic, got1, payload, qos, dup, _ = fb.parse_publish(first, body)
        assert (topic, got1, payload, qos, dup) == ("tm/r1", pid1, b"one", 1, False)
        pid2 = d.publish(2, "tm/r2", "two")
        first, body = fake.recv_packet()
        _, got2, _, qos, _, _ = fb.parse_publish(first, body)
        assert (got2, qos) == (pid2, 2)
        fake.send(fb.ack(5, pid2))  # PUBREC
        first, body = fake.recv_packet()
        assert first == 0x62 and body == pid2.to_bytes(2, "big")  # PUBREL
        assert d.status()["inflight"] == 2

        fake.drop()
        d.expect(r"CLOSED")
        d.send("CONNECT")
        fake.accept()
        assert fake.recv_packet()[0] == 0x10
        fake.send(fb.connack(True))
        d.expect(r"CONNECT ok")
        first, body = fake.recv_packet()
        topic, got, payload, qos, dup, _ = fb.parse_publish(first, body)
        assert (topic, got, payload, qos, dup) == ("tm/r1", pid1, b"one", 1, True), "QoS 1 resent with DUP"
        first, body = fake.recv_packet()
        assert first == 0x62 and body == pid2.to_bytes(2, "big"), "PUBREL resent"
        fake.send(fb.ack(4, pid1))  # PUBACK
        d.expect(rf"PUBDONE {pid1} 00")
        fake.send(fb.ack(7, pid2))  # PUBCOMP
        d.expect(rf"PUBDONE {pid2} 00")
        assert d.status()["inflight"] == 0

        # without a session on the server, nothing is resent
        d.publish(1, "tm/r3", "three")
        fake.recv_packet()
        fake.drop()
        d.expect(r"CLOSED")
        d.send("CONNECT")
        fake.accept()
        fake.recv_packet()
        fake.send(fb.connack(False))
        d.expect(r"CONNECT ok")
        assert d.status()["inflight"] == 0
        d.disconnect()
    finally:
        fake.close()


def scenario_qos2_in(d):
    """An incoming QoS 2 message resent before PUBREL is delivered once."""
    fake = fb.FakeBroker(d.bind)
    try:
        d.reset(fake.port)
        d.send("CONNECT")
        fake.accept()
        fake.recv_packet()
        fake.send(fb.connack(False))
        d.expect(r"CONNECT ok")
        fake.send(fb.publish("tm/q2", b"once", qos=2, packet_id=7))
        assert d.message()["payload"] == "once"
        assert fake.recv_packet() == (0x50, b"\x00\x07")  # PUBREC
        fake.send(fb.publish("tm/q2", b"once", qos=2, packet_id=7, dup=True))
        assert fake.recv_packet() == (0x50, b"\x00\x07")
        d.no_message(0.5)
        fake.send(fb.ack(6, 7))  # PUBREL
        assert fake.recv_packet() == (0x70, b"\x00\x07")  # PUBCOMP
        # the identifier can be used again
        fake.send(fb.publish("tm/q2", b"again", qos=2, packet_id=7))
        assert d.message()["payload"] == "again"
        d.disconnect()
    finally:
        fake.close()


def scenario_ping_timeout(d):
    """No PINGRESP within the keep alive closes the connection."""
    fake = fb.FakeBroker(d.bind)
    try:
        d.reset(fake.port, keepalive=1)
        d.send("CONNECT")
        fake.accept()
        fake.recv_packet()
        fake.send(fb.connack(False))
        d.expect(r"CONNECT ok")
        assert fake.recv_packet() == (0xC0, b"")  # PINGREQ after 1 s
        d.expect(r"^CLOSED 8D", timeout=5)
        assert fake.expect_closed()
    finally:
        fake.close()


def scenario_refused(d):
    fake = fb.FakeBroker(d.bind)
    try:
        d.reset(fake.port)
        d.send("CONNECT")
        fake.accept()
        fake.recv_packet()
        fake.send(fb.connack(False, code=2))  # identifier rejected
        m = d.expect(r"CONNECT fail ([0-9A-F]{2})")
        assert int(m.group(1), 16) == 0x85
    finally:
        fake.close()


def test_client_interop(dut, record_property):
    d = Dut(dut)
    ip = d.ready()
    d.server = target.pc_address_for(ip)  # mosquitto and the fake broker run on the PC
    d.bind = target.bind_address_for(ip)
    results = {}

    def run(name, fn, *args):
        try:
            fn(d, *args)
            results[name] = "PASS"
        except Exception as e:  # report every scenario, then fail at the end
            results[name] = f"FAIL {type(e).__name__}: {e}"
            d.send("DISCONNECT")
        print(f"SCENARIO {results[name].split()[0]} {name} {results[name][4:]}")
        record_property(f"scenario_{name}", results[name])

    with mosquitto(OUTPUT, bind=d.bind) as (port, log_path):
        run("pubsub", scenario_pubsub, port)
        run("retain_unsubscribe", scenario_retain_unsubscribe, port)
        run("will", scenario_will, port)
        run("persistent", scenario_persistent, port)
        run("keepalive", scenario_keepalive, port, log_path)
        run("oversize", scenario_oversize, port)
        run("mqtt31", scenario_mqtt31, port)
        for name, fn in V5_SCENARIOS:
            run(name, fn, port)
    run("auth", scenario_auth)
    run("resend", scenario_resend)
    run("qos2_in", scenario_qos2_in)
    run("ping_timeout", scenario_ping_timeout)
    run("refused", scenario_refused)

    failed = {k: v for k, v in results.items() if v != "PASS"}
    assert not failed, failed
