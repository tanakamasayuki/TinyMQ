"""A paho-mqtt client on the PC side of the interop tests, with a queue of
received messages to wait on."""

import queue
import time

import paho.mqtt.client as mqtt


class PahoPeer:
    def __init__(self, port, client_id="pc", protocol=mqtt.MQTTv311, clean_session=True, host="127.0.0.1"):
        self.messages = queue.Queue()
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id, protocol=protocol, clean_session=clean_session)
        self.client.on_message = lambda c, u, m: self.messages.put(m)
        self.client.connect(host, port, keepalive=30)
        self.client.loop_start()
        deadline = time.time() + 5
        while not self.client.is_connected():
            if time.time() > deadline:
                raise TimeoutError("paho did not connect")
            time.sleep(0.01)

    def subscribe(self, topic, qos=0):
        result, mid = self.client.subscribe(topic, qos)
        assert result == mqtt.MQTT_ERR_SUCCESS
        time.sleep(0.2)  # let SUBACK arrive before anything is published

    def publish(self, topic, payload, qos=0, retain=False):
        info = self.client.publish(topic, payload, qos=qos, retain=retain)
        info.wait_for_publish(5)
        return info

    def expect(self, timeout=5):
        """Returns the next received message or raises TimeoutError."""
        try:
            return self.messages.get(timeout=timeout)
        except queue.Empty:
            raise TimeoutError("no message from the broker") from None

    def expect_none(self, wait=1.0):
        try:
            m = self.messages.get(timeout=wait)
        except queue.Empty:
            return
        raise AssertionError(f"unexpected message on {m.topic}: {m.payload!r}")

    def close(self):
        self.client.disconnect()
        self.client.loop_stop()
