"""The Eclipse paho.mqtt.testing interoperability suites (MQTT 3.1.1 and 5.0)
against TinyMQBroker.

The suites are fetched by tools/fetch_paho_testing.sh into
tests/.tools/paho.mqtt.testing (a test tool, not part of the library); the
test is skipped without them, and unless TINYMQ_RUN_PAHO_TESTING=1 says to
run that third-party code. Each suite runs as a subprocess and its unittest
result decides; a suite that ran no tests fails.
"""

import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

from helpers import target
from helpers.line_dut import LineDut
from helpers.mosquitto import free_port

SUITES = Path(__file__).resolve().parent.parent / ".tools" / "paho.mqtt.testing" / "interoperability"
OUTPUT = Path(__file__).parent / "output"


def run_suite(script, host, port):
    runner = Path(__file__).resolve().parent.parent / "helpers" / "run_paho_suite.py"
    proc = subprocess.run([sys.executable, str(runner), script, host, str(port)], cwd=SUITES, capture_output=True, text=True, timeout=900)
    OUTPUT.mkdir(exist_ok=True)
    (OUTPUT / f"{script}.log").write_text(proc.stdout + proc.stderr)
    summary = re.findall(r"^(Ran \d+ tests.*|OK.*|FAILED.*)$", proc.stdout + proc.stderr, re.M)
    failures = re.findall(r"^(FAIL|ERROR): (\S+)", proc.stdout + proc.stderr, re.M)
    return proc.returncode, summary, failures


def test_broker_conformance(dut, record_property):
    if os.environ.get("TINYMQ_RUN_PAHO_TESTING") != "1":
        pytest.skip("runs third-party code: set TINYMQ_RUN_PAHO_TESTING=1 to run it")
    if not (SUITES / "client_test.py").exists():
        pytest.skip("paho.mqtt.testing not found (run tests/tools/fetch_paho_testing.sh)")
    d = LineDut(dut)
    ip = d.ready()
    port = free_port() if target.is_local(ip) else 1883
    d.send(f"LISTEN {port}")
    d.expect(rf"^LISTENING {port}")
    results = {}
    for script in ("client_test.py", "client_test5.py"):
        code, summary, failures = run_suite(script, ip, port)
        ran = int(re.search(r"Ran (\d+) tests", " ".join(summary)).group(1)) if summary else 0
        if ran == 0:
            code = code or 1  # nothing ran: not a pass
        results[script] = (code, failures)
        print(f"SUITE {script} exit={code} {' / '.join(summary)} failures={failures}")
        record_property(f"suite_{script}", f"exit={code} {failures}")
    failed = {k: v for k, v in results.items() if v[0] != 0}
    assert not failed, f"see {OUTPUT}/*.log: {failed}"
