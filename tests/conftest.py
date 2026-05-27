"""
pytest configuration for lmttfy tests.

Ensures the ``src`` directory is on ``sys.path`` so the ``lmttfy`` package
is importable, and switches ``multiprocessing`` to the ``fork`` start method
so functions-under-test do not need to be picklable by module reference.
"""

import multiprocessing
import sys
from pathlib import Path


# add the src directory to sys.path so that lmttfy can be imported
_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

# use fork so that multiprocessing.Process inherits memory from the parent,
# avoiding pickling issues for functions defined inside tests.
#
# force=True lets us override the forkserver/spawn default on Python 3.14+.
multiprocessing.set_start_method("fork", force=True)
