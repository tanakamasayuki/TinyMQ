"""Runs the MQTT 5.0 property unit tests on the DUT and checks every EXPECT passed."""

import re


def test_props(dut):
    match = dut.expect(re.compile(rb"TEST done (\d+)/(\d+)"), timeout=60)
    passed = int(match.group(1))
    total = int(match.group(2))
    assert passed == total, f"{passed}/{total} EXPECTs passed (see PASS/FAIL lines above)"
