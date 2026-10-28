"""Runs a paho.mqtt.testing interoperability script against a broker.

usage: python run_paho_suite.py <script> <host> <port>

The scripts read -h / -p themselves and then call unittest.main(), which
reads sys.argv again and takes -h for its own help. Here unittest.main()
gets only the script name, so the options reach the script alone.
"""

import runpy
import sys
import unittest

script, host, port = sys.argv[1:4]
sys.path.insert(0, ".")  # the suite imports its own mqtt package from its folder
original = unittest.main


def main(*args, **kwargs):
    kwargs["argv"] = [script]
    return original(*args, **kwargs)


unittest.main = main
sys.argv = [script, "-h", host, "-p", port]
runpy.run_path(script, run_name="__main__")
