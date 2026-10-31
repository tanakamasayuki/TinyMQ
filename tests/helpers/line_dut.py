"""Line-oriented access to a DUT driven by serial commands.

Output is read line by line; lines that do not match what is awaited are
kept, so an event printed earlier than expected is still found by a later
expect().
"""

import re
import time


class LineDut:
    def __init__(self, dut):
        self.dut = dut
        self.backlog = []

    def send(self, line):
        self.dut.write(line + "\n")

    def _next_line(self, timeout):
        m = self.dut.expect(re.compile(rb"([^\r\n]*)\r?\n"), timeout=timeout)
        return m.group(1).decode(errors="replace")

    def expect(self, pattern, timeout=10):
        rx = re.compile(pattern)
        for i, line in enumerate(self.backlog):
            m = rx.search(line)
            if m:
                del self.backlog[: i + 1]
                return m
        deadline = time.time() + timeout
        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                raise TimeoutError(f"no line matching {pattern!r}; unmatched: {self.backlog[-5:]}")
            try:
                line = self._next_line(remaining)
            except Exception:
                raise TimeoutError(f"no line matching {pattern!r}; unmatched: {self.backlog[-5:]}") from None
            m = rx.search(line)
            if m:
                return m
            self.backlog.append(line)

    def setting(self, line):
        """Sends a settings command and waits for its OK <command>."""
        self.send(line)
        self.expect(rf"^OK {line.split()[0]}$")

    def ready(self, timeout=150):
        """Waits for READY <ip> (after joining Wi-Fi on a board) and returns the ip."""
        m = self.expect(r"^(READY|NET_ERROR) ?(.*)$", timeout)
        assert m.group(1) == "READY", f"the board has no network: {m.group(2)}"
        # Ends any stray byte the UART picked up at reset (seen on the
        # ESP32-P4's CH340), so it does not prefix the first command.
        self.send("")
        return m.group(2).strip()

    def expect_none(self, pattern, wait=1.0):
        try:
            m = self.expect(pattern, timeout=wait)
        except TimeoutError:
            return
        raise AssertionError(f"unexpected {m.group(0)!r}")
