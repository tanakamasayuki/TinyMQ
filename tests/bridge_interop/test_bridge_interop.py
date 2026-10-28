"""TinyMQBridge: the DUT's broker bridged to mosquitto on the PC, one bridge on
MQTT 5.0 (or 3.1.1) and one on 3.1.1. paho-mqtt clients on both sides check
what is forwarded, the prefixes, that nothing loops, the retained messages
taken upstream, and the recovery after mosquitto restarts.

The host DUT accepts one serial connection per process, so the scenarios
run inside one test function and are reported one by one.
"""

import re
import time
from pathlib import Path

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from helpers import target
from helpers.line_dut import LineDut
from helpers.mosquitto import free_port, mosquitto
from helpers.paho_peer import PahoPeer

OUTPUT = Path(__file__).parent / "output"


class Dut(LineDut):
    def stats(self):
        self.send("STATS")
        m = self.expect(r"^BSTATS a=(\d) out=(\d+) in=(\d+) dropped=(\d+) b=(\d) out=(\d+) in=(\d+)")
        keys = ("a", "out", "in", "dropped", "b", "out_b", "in_b")
        return dict(zip(keys, (int(g) for g in m.groups())))

    def wait_bridges(self, a=1, b=1, timeout=10):
        deadline = time.time() + timeout
        while True:
            s = self.stats()
            if (s["a"], s["b"]) == (a, b):
                return s
            if time.time() > deadline:
                raise AssertionError(f"bridges {s['a']}/{s['b']}, expected {a}/{b}")
            time.sleep(0.2)


def local(port, client_id, **kw):
    return PahoPeer(port, client_id=client_id, **kw)  # target.HOST: the DUT


def remote(port, client_id, **kw):
    return PahoPeer(port, client_id=client_id, host="127.0.0.1", **kw)


def scenario_retained_upstream(d, lport, rport):
    """A retained message published before the bridge connected is taken upstream."""
    r = remote(rport, "r-ret")
    try:
        r.subscribe("edge1/ret/#", 1)
        m = r.expect()
        assert (m.topic, m.payload, m.retain) == ("edge1/ret/early", b"v-early", True), (m.topic, m.payload, m.retain)
    finally:
        r.close()


def scenario_out(d, lport, rport):
    l, r = local(lport, "l-out", protocol=mqtt.MQTTv5), remote(rport, "r-out", protocol=mqtt.MQTTv5)
    try:
        r.subscribe("edge1/out/#", 2)
        r.subscribe("edge1/s/#", 2)
        props = Properties(PacketTypes.PUBLISH)
        props.UserProperty = [("site", "1")]
        props.ContentType = "text/plain"
        l.publish("out/a", "hello", qos=1, properties=props)
        m = r.expect()
        assert (m.topic, m.payload, m.qos) == ("edge1/out/a", b"hello", 1), (m.topic, m.payload, m.qos)
        assert m.properties.UserProperty == [("site", "1")] and m.properties.ContentType == "text/plain", m.properties
        l.publish("sensors/k/temp", "21.5", qos=1)
        m = r.expect()
        assert (m.topic, m.payload, m.qos) == ("edge1/s/k/temp", b"21.5", 0), (m.topic, m.qos)  # QoS of the map
        l.publish("sensors/k/hum", "40", qos=1)
        l.publish("other/x", "no", qos=1)
        r.expect_none(0.8)
    finally:
        l.close()
        r.close()


def scenario_in(d, lport, rport):
    l, r = local(lport, "l-in"), remote(rport, "r-in")
    try:
        l.subscribe("cmd/#", 1)
        r.subscribe("edge1/cmd/#", 1)
        r.subscribe("edge1/elsewhere", 1)
        r.publish("edge1/cmd/led", "on", qos=1)
        assert r.expect().topic == "edge1/cmd/led"  # its own
        m = l.expect()
        assert (m.topic, m.payload, m.qos) == ("cmd/led", b"on", 1)
        r.expect_none(0.8)  # not sent back out
        r.publish("edge1/elsewhere", "no", qos=1)
        r.expect()
        l.expect_none(0.5)
    finally:
        l.close()
        r.close()


def scenario_both_no_loop(d, lport, rport):
    l, r = local(lport, "l-sync"), remote(rport, "r-sync")
    try:
        l.subscribe("sync/#", 1)
        r.subscribe("sync/#", 1)
        l.publish("sync/local", "from-local", qos=1)
        assert [m.payload for m in (l.expect(), r.expect())] == [b"from-local", b"from-local"]
        r.publish("sync/remote", "from-remote", qos=1)
        assert {m.payload for m in (l.expect(), r.expect())} == {b"from-remote"}
        l.expect_none(1.0)
        r.expect_none(0.2)
    finally:
        l.close()
        r.close()


def scenario_bridge_311(d, lport, rport):
    l, r = local(lport, "l-311"), remote(rport, "r-311")
    try:
        r.subscribe("edge2/o2/#", 1)
        l.subscribe("c2/#", 1)
        l.publish("o2/a", "up", qos=1)
        m = r.expect()
        assert (m.topic, m.payload) == ("edge2/o2/a", b"up")
        r.publish("edge2/c2/z", "down", qos=1)
        m = l.expect()
        assert (m.topic, m.payload) == ("c2/z", b"down")
        s = d.stats()
        assert s["out_b"] >= 1 and s["in_b"] >= 1, s
    finally:
        l.close()
        r.close()


def test_bridge_interop(dut, record_property):
    d = Dut(dut)
    ip = d.ready()
    pc = target.pc_address_for(ip)
    bind = target.bind_address_for(ip)
    lport = free_port() if target.is_local(ip) else 1883
    if not target.is_local(ip):
        target.HOST = ip
    results = {}

    def run(name, fn, *args):
        try:
            fn(d, *args)
            results[name] = "PASS"
        except Exception as e:  # report every scenario, then fail at the end
            results[name] = f"FAIL {type(e).__name__}: {e}"
        print(f"SCENARIO {results[name].split()[0]} {name} {results[name][4:]}")
        record_property(f"scenario_{name}", results[name])

    try:
        d.setting(f"BROKER {lport}")
        with mosquitto(OUTPUT, bind=bind) as (rport, _):
            d.send("LPUB 1 1 ret/early v-early")  # before the bridge exists
            d.expect("^LPUB ok")
            d.setting(f"BRIDGE {pc} {rport} 5")
            d.setting(f"BRIDGE2 {pc} {rport}")
            d.wait_bridges()
            time.sleep(0.5)  # subscriptions upstream settle
            run("retained_upstream", scenario_retained_upstream, lport, rport)
            run("out", scenario_out, lport, rport)
            run("in", scenario_in, lport, rport)
            run("both_no_loop", scenario_both_no_loop, lport, rport)
            run("bridge_311", scenario_bridge_311, lport, rport)

        # upstream gone: messages out are dropped, the bridges try again
        def down(d):
            s = d.wait_bridges(0, 0)
            before = s["dropped"]
            l = local(lport, "l-down")
            l.publish("out/lost", "x", qos=1)
            l.close()
            time.sleep(0.3)
            assert d.stats()["dropped"] == before + 1

        run("upstream_down", down)

        # back on the same port (no persistence: nothing kept upstream)
        def back(d):
            with mosquitto(OUTPUT, bind=bind, port=rport):
                d.wait_bridges(1, 1, timeout=15)
                time.sleep(0.5)
                scenario_retained_upstream(d, lport, rport)  # taken up again on reconnect
                scenario_in(d, lport, rport)                 # subscribed again upstream
                scenario_out(d, lport, rport)

        run("reconnect", back)
    finally:
        target.HOST = "127.0.0.1"

    failed = {k: v for k, v in results.items() if v != "PASS"}
    assert not failed, failed
