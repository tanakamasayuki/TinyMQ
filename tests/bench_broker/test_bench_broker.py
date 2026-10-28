"""Measurements of TinyMQBroker from the PC with paho-mqtt: round-trip
latency, one-to-one throughput and one-to-four fan-out, through each
listener the DUT has. Values are printed as BENCH <name> <value> <unit> and
kept as junit properties; the test fails only if QoS 1 messages go missing
(QoS 0 may be dropped when a subscriber falls behind: the share delivered
is recorded).

    uv run --env-file .env pytest bench_broker --profile esp32 -s
"""

import statistics
import threading
import time

import paho.mqtt.client as mqtt

from helpers import target
from helpers.line_dut import LineDut
from helpers.mosquitto import free_port


class Peer:
    def __init__(self, port, client_id):
        self.received = 0
        self.last = None
        self.event = threading.Event()
        self.c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
        self.c.max_inflight_messages_set(20)
        self.c.on_message = self._on_message
        self.c.connect(target.HOST, port, keepalive=60)
        self.c.loop_start()
        deadline = time.time() + 5
        while not self.c.is_connected():
            assert time.time() < deadline, "no connection"
            time.sleep(0.01)

    def _on_message(self, c, u, m):
        self.received += 1
        self.last = time.perf_counter()
        self.event.set()

    def subscribe(self, topic, qos):
        self.c.subscribe(topic, qos)
        time.sleep(0.3)

    def close(self):
        self.c.disconnect()
        self.c.loop_stop()


def latency(port, qos, n=200):
    pub, sub = Peer(port, "lat-pub"), Peer(port, "lat-sub")
    try:
        sub.subscribe("bench/lat", qos)
        times = []
        for i in range(n):
            sub.event.clear()
            t0 = time.perf_counter()
            pub.c.publish("bench/lat", b"x" * 32, qos=qos)
            assert sub.event.wait(3), f"message {i} lost"
            times.append((time.perf_counter() - t0) * 1000)
        times.sort()
        return statistics.median(times), times[int(len(times) * 0.95)]
    finally:
        pub.close()
        sub.close()


def throughput(port, qos, subscribers=1, n=1000, size=100):
    pub = Peer(port, "tp-pub")
    subs = [Peer(port, f"tp-sub{i}") for i in range(subscribers)]
    try:
        for s in subs:
            s.subscribe("bench/tp", qos)
        payload = b"x" * size
        t0 = time.perf_counter()
        for _ in range(n):
            info = pub.c.publish("bench/tp", payload, qos=qos)
            if qos:
                info.wait_for_publish(5)  # paho keeps at most 20 in flight
        deadline = time.time() + 30
        while sum(s.received for s in subs) < n * subscribers and time.time() < deadline:
            time.sleep(0.01)
        got = sum(s.received for s in subs)
        end = max(s.last or t0 for s in subs)
        return got, got / (end - t0)
    finally:
        pub.close()
        for s in subs:
            s.close()


def test_bench_broker(dut, record_property):
    d = LineDut(dut)
    ip = d.ready()
    port = free_port() if target.is_local(ip) else 1883
    if not target.is_local(ip):
        target.HOST = ip
    d.send(f"LISTEN {port}")
    m = d.expect(rf"^LISTENING {port}( lwip=(\d+))?")
    listeners = [("tcp", port)] + ([("lwip", int(m.group(2)))] if m.group(2) else [])
    missing = []

    def bench(name, value, unit):
        print(f"BENCH {name} {value:.2f} {unit}")
        record_property(name, f"{value:.2f} {unit}")

    try:
        for kind, p in listeners:
            for qos in (0, 1):
                med, p95 = latency(p, qos)
                bench(f"{kind}_latency_qos{qos}_median", med, "ms")
                bench(f"{kind}_latency_qos{qos}_p95", p95, "ms")
            # QoS 0 may be dropped when a subscriber's queue is full: the share
            # delivered is recorded. QoS 1 must all arrive.
            got, rate = throughput(p, 0, n=2000)
            bench(f"{kind}_throughput_qos0", rate, "msg/s")
            bench(f"{kind}_delivered_qos0", 100 * got / 2000, "%")
            got, rate = throughput(p, 1, n=500)
            bench(f"{kind}_throughput_qos1", rate, "msg/s")
            if got < 500:
                missing.append(f"{kind} qos1: {got}/500")
            got, rate = throughput(p, 0, subscribers=4, n=500)
            bench(f"{kind}_fanout4_qos0", rate, "deliveries/s")
            bench(f"{kind}_fanout4_delivered", 100 * got / 2000, "%")
        d.send("STATS")
        print(d.expect(r"^STATS .*").group(0))
    finally:
        target.HOST = "127.0.0.1"
    assert not missing, missing
