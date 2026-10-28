"""MQTT-SN gateway scenarios, run by test_broker_interop.py against the same DUT.

Each takes (d, port, sn_port): the serial helper, the broker's TCP port and
the gateway's UDP port. The DUT predefines topic id 1 = "pre/one" and
2 = "pre/two".
"""

import socket
import time

from helpers import sn_peer as snp
from helpers.paho_peer import PahoPeer


def scenario_sn_pubsub(d, port, sn_port):
    b = PahoPeer(port, client_id="b")
    s = snp.SnPeer(sn_port)
    try:
        b.subscribe("sn/#", 2)
        assert s.connect("sn1") == 0
        tid = s.register("sn/temp")
        assert s.register("sn/temp") == tid, "registering again gives the same id"
        for qos in (0, 1, 2):
            assert s.publish(tid, f"t{qos}", qos=qos) == 0
            m = b.expect()
            assert (m.topic, m.payload, m.qos) == ("sn/temp", f"t{qos}".encode(), qos)
        # retained from MQTT-SN, seen by a later TCP subscriber
        s.publish(tid, "kept", qos=1, retain=True)
        b.expect()
        c = PahoPeer(port, client_id="c")
        c.subscribe("sn/temp", 0)
        m = c.expect()
        assert m.retain and m.payload == b"kept"
        c.close()
        s.publish(tid, "", qos=1, retain=True)
        b.expect()
        s.disconnect()
    finally:
        b.close()
        s.close()


def scenario_sn_subscribe(d, port, sn_port):
    a = PahoPeer(port, client_id="a")
    s = snp.SnPeer(sn_port)
    try:
        assert s.connect("sn2") == 0
        qos, tid, rc = s.subscribe("cmd/led", 1)
        assert (qos, rc) == (1, 0) and tid != 0
        a.publish("cmd/led", "on", qos=1)
        m = s.receive()
        assert (m["topic_id"], m["data"], m["qos"], m["topic_type"]) == (tid, b"on", 1, snp.NORMAL), m

        # a wildcard: SUBACK has no topic id, the gateway registers each topic first
        qos, tid2, rc = s.subscribe("cmd/#", 2)
        assert (qos, tid2, rc) == (2, 0, 0)
        a.publish("cmd/x/y", "deep", qos=2)
        m = s.receive()
        assert (m["topic"], m["data"], m["qos"]) == ("cmd/x/y", b"deep", 2), m
        a.publish("cmd/x/y", "again", qos=0)
        m = s.receive()
        assert (m["topic"], m["data"]) == ("cmd/x/y", b"again"), "registered once"
        # cmd/led matches both subscriptions: one delivery
        a.publish("cmd/led", "both", qos=1)
        assert s.receive()["data"] == b"both"
        s.expect_nothing(0.5)

        # short topic names
        assert s.subscribe("ab", 1, snp.SHORT)[2] == 0
        a.publish("ab", "short", qos=1)
        m = s.receive()
        assert (m["topic_type"], m["topic"], m["data"]) == (snp.SHORT, "ab", b"short"), m
        s.publish(snp.short_id("ab"), "from-sn", qos=1, topic_type=snp.SHORT)
        assert s.receive()["data"] == b"from-sn"  # subscribed to itself

        s.unsubscribe("cmd/#")
        s.unsubscribe("cmd/led")
        a.publish("cmd/x/y", "unheard", qos=1)
        s.expect_nothing(0.5)
        s.disconnect()
    finally:
        a.close()
        s.close()


def scenario_sn_predefined(d, port, sn_port):
    b = PahoPeer(port, client_id="b")
    s = snp.SnPeer(sn_port)
    try:
        b.subscribe("pre/#", 1)
        assert s.connect("sn3") == 0
        s.publish(1, "via-id", qos=1, topic_type=snp.PREDEFINED)
        m = b.expect()
        assert (m.topic, m.payload) == ("pre/one", b"via-id")
        qos, tid, rc = s.subscribe(2, 1, snp.PREDEFINED)
        assert (tid, rc) == (2, 0)
        b.publish("pre/two", "to-sn", qos=1)
        m = s.receive()
        assert (m["topic_type"], m["topic_id"], m["data"]) == (snp.PREDEFINED, 2, b"to-sn"), m
        b.expect()  # b sees its own message too
        # unknown predefined id
        assert s.subscribe(99, 0, snp.PREDEFINED)[2] == 0x02
        assert s.publish(99, "x", qos=1, topic_type=snp.PREDEFINED) == 0x02
        # an unregistered normal topic id
        assert s.publish(1234, "x", qos=1) == 0x02
        s.disconnect()
    finally:
        b.close()
        s.close()


def scenario_sn_qos_minus_one(d, port, sn_port):
    b = PahoPeer(port, client_id="b")
    s = snp.SnPeer(sn_port)  # never connects
    try:
        b.subscribe("#", 0)
        s.publish(1, "no-connection", qos=-1, topic_type=snp.PREDEFINED)
        m = b.expect()
        assert (m.topic, m.payload) == ("pre/one", b"no-connection")
        s.publish(snp.short_id("zz"), "short", qos=-1, topic_type=snp.SHORT)
        m = b.expect()
        assert (m.topic, m.payload) == ("zz", b"short")
        s.publish(1, "normal id", qos=-1, topic_type=snp.NORMAL)  # not allowed: ignored
        b.expect_none(0.5)
        # QoS 0 without a connection is answered with DISCONNECT
        s.publish(1, "x", qos=0, topic_type=snp.PREDEFINED)
        s.expect(snp.DISCONNECT)
    finally:
        b.close()
        s.close()


def scenario_sn_will(d, port, sn_port):
    b = PahoPeer(port, client_id="b")
    s = snp.SnPeer(sn_port)
    try:
        b.subscribe("snw/#", 1)
        assert s.connect("sn-will", duration=1, will=("snw/1", "first", 1, False)) == 0
        s.send(snp.WILLMSGUPD, b"gone")
        assert s.expect(snp.WILLMSGRESP) == b"\x00"
        s.send(snp.WILLTOPICUPD, bytes([snp.flags(qos=1)]) + b"snw/2")
        assert s.expect(snp.WILLTOPICRESP) == b"\x00"
        # silent past 1.5 x the keep alive: lost
        m = b.expect(timeout=5)
        assert (m.topic, m.payload, m.qos) == ("snw/2", b"gone", 1)
        assert d.stats()["sn"] == 0
        # a normal DISCONNECT discards the will
        assert s.connect("sn-will", will=("snw/1", "bye", 0, False)) == 0
        s.disconnect()
        b.expect_none(0.5)
        # an empty WILLTOPIC means no will
        s.send(snp.CONNECT, bytes([snp.flags(clean=True, will=True), 1, 0, 60]) + b"sn-will")
        s.expect(snp.WILLTOPICREQ)
        s.send(snp.WILLTOPIC)
        assert s.expect(snp.CONNACK) == b"\x00"
        s.disconnect()
    finally:
        b.close()
        s.close()


def scenario_sn_keepalive(d, port, sn_port):
    s = snp.SnPeer(sn_port)
    try:
        assert s.connect("sn-ka", duration=1) == 0
        for _ in range(4):  # PINGREQ keeps it alive past the 1.5 s limit
            time.sleep(0.6)
            s.ping()
            s.expect(snp.PINGRESP)
        assert d.stats()["sn"] == 1
        time.sleep(2)
        assert d.stats()["sn"] == 0
        s.expect(snp.DISCONNECT)  # told when it was dropped
    finally:
        s.close()


def scenario_sn_sleep(d, port, sn_port):
    a = PahoPeer(port, client_id="a")
    s = snp.SnPeer(sn_port)
    try:
        assert s.connect("sleeper", clean=False) == 0
        s.subscribe("sl/#", 1)
        s.disconnect(duration=30)  # asleep
        a.publish("sl/a", "one", qos=1)
        a.publish("sl/b", "two", qos=0)  # kept for a sleeping client too
        s.expect_nothing(0.5)
        s.ping("sleeper")  # wake up: messages, then PINGRESP
        got = [s.receive(), s.receive()]
        assert [(m["topic"], m["data"]) for m in got] == [("sl/a", b"one"), ("sl/b", b"two")], got
        s.expect(snp.PINGRESP)
        a.publish("sl/a", "three", qos=1)
        s.expect_nothing(0.5)
        s.ping("sleeper")
        assert s.receive()["data"] == b"three"
        s.expect(snp.PINGRESP)
        s.ping("sleeper")  # nothing waiting
        s.expect(snp.PINGRESP)
        # CONNECT ends the sleep
        assert s.connect("sleeper", clean=False) == 0
        a.publish("sl/a", "awake", qos=1)
        assert s.receive()["data"] == b"awake"
        s.disconnect()
        s.connect("sleeper", clean=True)  # leave no session
        s.disconnect()
    finally:
        a.close()
        s.close()


def scenario_sn_resend(d, port, sn_port):
    """Unacknowledged deliveries over UDP are resent, and a silent client is dropped."""
    a = PahoPeer(port, client_id="a")
    s = snp.SnPeer(sn_port)
    d.setting("SNRETRY 400 2")
    try:
        assert s.connect("sn-rs") == 0
        s.subscribe("rs2/x", 1)
        a.publish("rs2/x", "lossy", qos=1)
        first = s.receive(ack=False)
        assert not first["dup"]
        again = s.receive(ack=False, timeout=2)
        assert (again["dup"], again["msg_id"], again["data"]) == (True, first["msg_id"], b"lossy")
        s.ack(again)
        s.expect_nothing(0.8)
        # never acknowledged: dropped after the resends
        a.publish("rs2/x", "ignored", qos=1)
        for _ in range(3):
            s.receive(ack=False, timeout=2)
        s.expect(snp.DISCONNECT, timeout=3)
        assert d.stats()["sn"] == 0
    finally:
        d.setting("SNRETRY 10000 3")
        a.close()
        s.close()


def scenario_sn_qos2_in(d, port, sn_port):
    b = PahoPeer(port, client_id="b")
    s = snp.SnPeer(sn_port)
    try:
        b.subscribe("q2sn", 2)
        assert s.connect("sn-q2") == 0
        tid = s.register("q2sn")
        body = bytes([snp.flags(qos=2)]) + tid.to_bytes(2, "big") + (7).to_bytes(2, "big") + b"once"
        s.send(snp.PUBLISH, body)
        s.expect(snp.PUBREC)
        s.send(snp.PUBLISH, bytes([snp.flags(qos=2, dup=True)]) + body[1:])  # PUBREC lost: resent
        s.expect(snp.PUBREC)
        s.send(snp.PUBREL, (7).to_bytes(2, "big"))
        s.expect(snp.PUBCOMP)
        assert b.expect().payload == b"once"
        b.expect_none(0.5)
        s.disconnect()
    finally:
        b.close()
        s.close()


def scenario_sn_persistent(d, port, sn_port):
    a = PahoPeer(port, client_id="a")
    s = snp.SnPeer(sn_port)
    try:
        assert s.connect("sn-p", clean=False) == 0
        s.subscribe("pp/x", 1)
        s.disconnect()
        a.publish("pp/x", "while-away", qos=1)
        s2 = snp.SnPeer(sn_port)  # back from another port
        try:
            assert s2.connect("sn-p", clean=False) == 0
            m = s2.receive()  # registered again first
            assert (m["topic"], m["data"]) == ("pp/x", b"while-away"), m
            s2.disconnect()
            s2.connect("sn-p", clean=True)
            s2.disconnect()
        finally:
            s2.close()
    finally:
        a.close()
        s.close()


def scenario_sn_connect_twice(d, port, sn_port):
    s = snp.SnPeer(sn_port)
    try:
        assert s.connect("sn-twice") == 0
        assert s.connect("sn-twice") == 0  # a resent CONNECT: CONNACK again
        assert d.stats()["sn"] == 1
        s.send(snp.SEARCHGW, b"\x01")
        assert s.expect(snp.GWINFO) == b"\x01"
        s.disconnect()
        assert d.stats()["sn"] == 0
        # protocol id other than 1
        s.send(snp.CONNECT, bytes([snp.flags(clean=True), 2, 0, 60]) + b"bad")
        assert s.expect(snp.CONNACK) == b"\x03"
    finally:
        s.close()


SCENARIOS = (
    ("sn_pubsub", scenario_sn_pubsub),
    ("sn_subscribe", scenario_sn_subscribe),
    ("sn_predefined", scenario_sn_predefined),
    ("sn_qos_minus_one", scenario_sn_qos_minus_one),
    ("sn_will", scenario_sn_will),
    ("sn_keepalive", scenario_sn_keepalive),
    ("sn_sleep", scenario_sn_sleep),
    ("sn_resend", scenario_sn_resend),
    ("sn_qos2_in", scenario_sn_qos2_in),
    ("sn_persistent", scenario_sn_persistent),
    ("sn_connect_twice", scenario_sn_connect_twice),
)


def free_udp_port():
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]
