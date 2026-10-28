"""Shared pytest hooks for TinyMQ tests.

Wipes each test's `output/` directory before it runs (as in PCMFlowUDP).
"""

import shutil
from pathlib import Path


def pytest_runtest_setup(item):
    output_dir = Path(item.fspath).parent / "output"
    if output_dir.exists():
        shutil.rmtree(output_dir)
