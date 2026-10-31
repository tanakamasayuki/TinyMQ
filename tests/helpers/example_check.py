"""For the tests of the examples (tests/example_*): the copy must be current."""

import subprocess
import sys
import time
from pathlib import Path

SYNC = Path(__file__).resolve().parents[1] / "tools" / "sync_examples.py"


def assert_copy_current():
    proc = subprocess.run([sys.executable, str(SYNC), "--check"], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stdout + proc.stderr


def reset_esp32(d):
    """Restarts an ESP32 through RTS (EN low, DTR high would select download
    mode); `d` is a LineDut. The S3 was seen not running its sketch after the
    upload. What the earlier boot printed is read and dropped first, so it is
    not taken for the new boot's lines."""
    while True:
        try:
            d._next_line(0.5)
        except Exception:
            break
    d.backlog.clear()
    port = d.dut.serial.proc
    port.dtr = False
    port.rts = True
    time.sleep(0.1)
    port.rts = False


def connect_retry(make, timeout=10):
    """make() until it connects: a sketch prints its address before it listens."""
    deadline = time.time() + timeout
    while True:
        try:
            return make()
        except OSError:
            if time.time() > deadline:
                raise
            time.sleep(0.5)
