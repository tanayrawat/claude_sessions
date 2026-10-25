"""Keeps the course honest: every lesson demo must run to completion. Run:  python -m pytest api-course -q"""
import subprocess
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
DEMOS = sorted(HERE.glob("[0-9][0-9]_*/demo.py"))


@pytest.mark.parametrize("demo", DEMOS, ids=lambda p: p.parent.name)
def test_demo_runs(demo):
    r = subprocess.run([sys.executable, str(demo)], capture_output=True, text=True, timeout=120)
    assert r.returncode == 0, r.stderr[-800:]
    assert "Traceback" not in r.stdout + r.stderr
