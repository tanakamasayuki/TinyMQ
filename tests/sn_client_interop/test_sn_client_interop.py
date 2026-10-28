"""TinyMQClientSn against the TinyMQBroker gateway in the same sketch (real UDP
sockets), checked from paho-mqtt over TCP, and against a scripted gateway
for lost and refused messages and the gateway search.

The host DUT accepts one serial connection per process, so the scenarios
run inside one test function and are reported one by one.
"""

import socket
import struct
import time

from helpers import sn_peer as snp
from helpers.line_dut import LineDut
from helpers.mosquitto import free_port
from helpers.paho_peer import PahoPeer


class Dut(LineDut):
    def ok(self, cmd, timeout=15):
        """Sends a command answered with '<NAME> ok|fail <rc>' and returns (ok, rc)."""
        self.send(cmd)
        name = cmd.split()[0]
        m = self.expect(rf"^{name} (ok|fail) (\d+)", timeout)
        return m.group(1) == "ok", int(m.group(2))

    def value(self, cmd, timeout=15):
        self.send(cmd)
        return int(self.expect(rf"^{cmd.split()[0]} (-?\d+)", timeout).group(1))

    def setting(self, cmd):
        self.send(cmd)
        self.expect(rf"^OK {cmd.split()[0]}")

    def message(self, timeout=10):
        m = self.expect(r"^MSG (\S+) (\d+) (\d) (-?\d) (\d) (\d) ?(.*)$", timeout)
        topic, tid, ttype, qos, retain, dup, payload = m.groups()
        return {"topic": topic, "topic_id": int(tid), "topic_type": int(ttype), "qos": int(qos), "retain": retain == "1", "dup": dup == "1", "payload": payload}

    def no_message(self, wait=0.8):
        self.expect_none(r"^MSG ", wait)


class FakeGateway:
    """A UDP socket standing in for the gateway."""

    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("127.0.0.1", 0))
        self.sock.settimeout(5)
        self.port = self.sock.getsockname()[1]
        self.peer = None

    def recv(self, timeout=5):
        self.sock.settimeout(timeout)
        data, self.peer = self.sock.recvfrom(4096)
        assert data[0] == len(data)
        return data[1], data[2:]

    def send(self, msg_type, body=b""):
        self.sock.sendto(snp.frame(msg_type, body), self.peer)

    def close(self):
        self.sock.close()


# ---- scenarios against the gateway in the sketch


def scenario_qos_minus_one(d, port, sn_port):
    pc = PahoPeer(port, client_id="pc")
    try:
        pc.subscribe("#", 0)
        assert d.ok("PUBPRE 1 -1 no-connection") == (True, 0)
        m = pc.expect()
        assert (m.topic, m.payload) == ("pre/one", b"no-connection")
        assert d.ok("PUBSHORT zz -1 short")[0]
        m = pc.expect()
        assert (m.topic, m.payload) == ("zz", b"short")
        assert d.ok("PUBPRE 1 1 needs-connection") == (False, 0)
    finally:
        pc.close()


def scenario_publish(d, port, sn_port):
    pc = PahoPeer(port, client_id="pc")
    try:
        pc.subscribe("snc/#", 2)
        assert d.ok("CONNECT") == (True, 0)
        for qos in (0, 1, 2):
            assert d.ok(f"PUB {qos} 0 snc/temp t{qos}") == (True, 0)
            m = pc.expect()
            assert (m.topic, m.payload, m.qos) == ("snc/temp", f"t{qos}".encode(), qos)
        tid = d.value("REG snc/temp")
        assert tid > 0 and d.value("REG snc/temp") == tid
        assert d.ok("PUB 1 1 snc/kept retained")[0]
        m = pc.expect()
        assert m.payload == b"retained"
        pc2 = PahoPeer(port, client_id="pc2")
        pc2.subscribe("snc/kept", 0)
        assert pc2.expect().retain
        pc2.close()
        assert d.ok("PUB 1 1 snc/kept ")[0]  # empty: deletes
        pc.expect()
        assert d.ok("PUBPRE 2 1 predefined")[0]
        assert d.ok("PUBSHORT ab 2 short-qos2")[0]
    finally:
        d.send("DISCONNECT")
        d.expect("^DISCONNECT done")
        pc.close()


def scenario_subscribe(d, port, sn_port):
    pc = PahoPeer(port, client_id="pc")
    try:
        assert d.ok("CONNECT")[0]
        assert d.value("SUB 1 cmd/led") == 1
        pc.publish("cmd/led", "on", qos=1)
        m = d.message()
        assert (m["topic"], m["qos"], m["payload"]) == ("cmd/led", 1, "on"), m
        # wildcard: the gateway registers the topic before the message
        assert d.value("SUB 2 cmd/#") == 2
        pc.publish("cmd/x/y", "deep", qos=2)
        m = d.message()
        assert (m["topic"], m["qos"], m["payload"]) == ("cmd/x/y", 2, "deep"), m
        d.no_message()
        assert d.value("SUBPRE 2 1") == 1
        pc.publish("pre/two", "pre", qos=1)
        m = d.message()
        assert (m["topic"], m["topic_id"], m["topic_type"], m["payload"]) == ("-", 2, 1, "pre"), m
        assert d.value("SUB 0 ab") == 0
        pc.publish("ab", "short", qos=0)
        m = d.message()
        assert (m["topic"], m["topic_type"]) == ("ab", 2), m
        assert d.ok("UNSUB cmd/#")[0]
        assert d.ok("UNSUB cmd/led")[0]
        pc.publish("cmd/x/y", "unheard", qos=1)
        d.no_message()
    finally:
        d.send("DISCONNECT")
        d.expect("^DISCONNECT done")
        pc.close()


def scenario_will_keepalive(d, port, sn_port):
    pc = PahoPeer(port, client_id="pc")
    try:
        pc.subscribe("sncw/#", 1)
        d.setting("WILL 1 0 sncw/1 client-gone")
        d.setting("KEEPALIVE 1")
        assert d.ok("CONNECT")[0]
        time.sleep(1.8)  # the sketch runs: PINGREQ keeps the client alive past 1.5 s
        d.send("STATUS")
        assert d.expect(r"^STATUS connected=(\d)").group(1) == "1"
        pc.expect_none(0.2)
        d.send("HOLD 2500")  # only the broker runs: the client falls silent
        d.expect("^HOLD done", 5)
        m = pc.expect(timeout=2)
        assert (m.topic, m.payload) == ("sncw/1", b"client-gone")
    finally:
        d.setting("NOWILL")
        d.setting("KEEPALIVE 60")
        pc.close()


def scenario_sleep(d, port, sn_port):
    pc = PahoPeer(port, client_id="pc")
    try:
        d.setting("ID sleeper")
        d.setting("CLEAN 0")
        assert d.ok("CONNECT")[0]
        assert d.value("SUB 1 sl/#") == 1
        assert d.ok("SLEEP 30")[0]
        d.send("STATUS")
        assert d.expect(r"^STATUS connected=(\d) asleep=(\d)").groups() == ("0", "1")
        pc.publish("sl/a", "one", qos=1)
        pc.publish("sl/b", "two", qos=0)
        d.send("HOLD 300")
        d.expect("^HOLD done")
        d.no_message(0.3)
        assert d.ok("CHECK")[0]
        got = sorted((m["topic"], m["payload"]) for m in (d.message(1), d.message(1)))
        assert got == [("sl/a", "one"), ("sl/b", "two")], got
        assert d.ok("CHECK")[0]  # nothing new
        d.no_message(0.3)
        assert d.ok("CONNECT")[0]
        pc.publish("sl/a", "awake", qos=1)
        assert d.message()["payload"] == "awake"
        d.send("DISCONNECT")
        d.expect("^DISCONNECT done")
        d.setting("CLEAN 1")
        assert d.ok("CONNECT")[0]  # leave no session
    finally:
        d.send("DISCONNECT")
        d.expect("^DISCONNECT done")
        d.setting("ID dut-sn")
        d.setting("CLEAN 1")
        pc.close()


# ---- scenarios against a scripted gateway


def connect_fake(d, fake):
    d.setting(f"GW 127.0.0.1 {fake.port}")
    d.send("CONNECT")
    t, body = fake.recv()
    assert t == snp.CONNECT and body[4:] == b"dut-sn", body
    fake.send(snp.CONNACK, b"\x00")
    assert d.expect(r"^CONNECT (ok|fail)").group(1) == "ok"


def scenario_resend(d, port, sn_port):
    fake = FakeGateway()
    try:
        d.setting("RETRY 300 3")
        connect_fake(d, fake)
        d.send("PUB 1 0 rs/x hello")
        t, body = fake.recv()
        assert t == snp.REGISTER
        tid_unused, mid = struct.unpack(">HH", body[:4])
        fake.send(snp.REGACK, struct.pack(">HHB", 5, mid, 0))
        t, body = fake.recv()
        assert t == snp.PUBLISH and not snp.parse_flags(body[0])["dup"]
        t, body2 = fake.recv(2)  # no PUBACK: resent with DUP
        f = snp.parse_flags(body2[0])
        assert t == snp.PUBLISH and f["dup"] and body2[1:5] == body[1:5]
        fake.send(snp.PUBACK, body[1:5] + b"\x00")
        assert d.expect(r"^PUB (ok|fail) (\d+)").groups() == ("ok", "0")

        # the gateway forgot the topic id: register again, publish again
        d.send("PUB 1 0 rs/x again")
        t, body = fake.recv()
        assert t == snp.PUBLISH
        fake.send(snp.PUBACK, body[1:5] + b"\x02")
        t, body = fake.recv()
        assert t == snp.REGISTER and body[4:] == b"rs/x"
        fake.send(snp.REGACK, struct.pack(">HHB", 6, struct.unpack(">H", body[2:4])[0], 0))
        t, body = fake.recv()
        assert t == snp.PUBLISH and struct.unpack(">H", body[1:3])[0] == 6
        fake.send(snp.PUBACK, body[1:5] + b"\x00")
        assert d.expect(r"^PUB (ok|fail)").group(1) == "ok"

        # no answer at all: fails after the resends
        t0 = time.time()
        d.send("REG never")
        for _ in range(4):
            assert fake.recv(2)[0] == snp.REGISTER
        assert int(d.expect(r"^REG (-?\d+)").group(1)) == -1
        assert time.time() - t0 < 3

        # an incoming message with an unknown topic id is refused
        fake.send(snp.PUBLISH, bytes([snp.flags(qos=1)]) + struct.pack(">HH", 77, 9) + b"x")
        t, body = fake.recv()
        assert t == snp.PUBACK and body == struct.pack(">HHB", 77, 9, 2)
        d.no_message(0.3)
        # QoS 2 delivered once though resent before PUBREL
        pub = bytes([snp.flags(qos=2, topic_type=snp.SHORT)]) + b"ab" + struct.pack(">H", 4) + b"q2"
        fake.send(snp.PUBLISH, pub)
        assert fake.recv() == (snp.PUBREC, b"\x00\x04")
        fake.send(snp.PUBLISH, bytes([pub[0] | 0x80]) + pub[1:])
        assert fake.recv() == (snp.PUBREC, b"\x00\x04")
        fake.send(snp.PUBREL, b"\x00\x04")
        assert fake.recv() == (snp.PUBCOMP, b"\x00\x04")
        m = d.message()
        assert (m["topic"], m["payload"], m["qos"]) == ("ab", "q2", 2)
        d.no_message(0.3)
        # the gateway drops the client
        fake.send(snp.DISCONNECT)
        time.sleep(0.2)
        d.send("STATUS")
        assert d.expect(r"^STATUS connected=(\d)").group(1) == "0"
    finally:
        d.setting("RETRY 10000 3")
        fake.close()


def scenario_refused(d, port, sn_port):
    fake = FakeGateway()
    try:
        d.setting(f"GW 127.0.0.1 {fake.port}")
        d.send("CONNECT")
        fake.recv()
        fake.send(snp.CONNACK, b"\x03")
        assert d.expect(r"^CONNECT (ok|fail) (\d+)").groups() == ("fail", "3")
    finally:
        fake.close()


def scenario_search(d, port, sn_port):
    fake = FakeGateway()
    try:
        d.send(f"SEARCH 127.0.0.1 {fake.port}")
        t, body = fake.recv()
        assert (t, body) == (snp.SEARCHGW, b"\x01")
        fake.send(snp.GWINFO, b"\x05")
        assert int(d.expect(r"^SEARCH (-?\d+)").group(1)) == 5
        # the answering address is now the gateway
        d.send("CONNECT")
        t, _ = fake.recv()
        assert t == snp.CONNECT
        fake.send(snp.CONNACK, b"\x00")
        assert d.expect(r"^CONNECT (ok|fail)").group(1) == "ok"
        d.send("DISCONNECT")
        assert fake.recv()[0] == snp.DISCONNECT
        fake.send(snp.DISCONNECT)
        d.expect("^DISCONNECT done")
    finally:
        fake.close()


def test_sn_client_interop(dut, record_property):
    d = Dut(dut)
    d.expect("^READY", timeout=30)
    port = free_port()
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.bind(("127.0.0.1", 0))
        sn_port = s.getsockname()[1]
    d.send(f"BROKER {port} {sn_port}")
    d.expect("^BROKER ready")
    results = {}

    def run(name, fn):
        d.setting(f"GW 127.0.0.1 {sn_port}")
        try:
            fn(d, port, sn_port)
            results[name] = "PASS"
        except Exception as e:  # report every scenario, then fail at the end
            results[name] = f"FAIL {type(e).__name__}: {e}"
        print(f"SCENARIO {results[name].split()[0]} {name} {results[name][4:]}")
        record_property(f"scenario_{name}", results[name])

    for name, fn in (
        ("qos_minus_one", scenario_qos_minus_one),
        ("publish", scenario_publish),
        ("subscribe", scenario_subscribe),
        ("will_keepalive", scenario_will_keepalive),
        ("sleep", scenario_sleep),
        ("resend", scenario_resend),
        ("refused", scenario_refused),
        ("search", scenario_search),
    ):
        run(name, fn)

    failed = {k: v for k, v in results.items() if v != "PASS"}
    assert not failed, failed
