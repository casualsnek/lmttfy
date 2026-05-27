"""
pytest configuration for lmttfy tests.

Ensures the ``src`` directory is on ``sys.path`` so the ``lmttfy`` package
is importable.

Multiprocessing start method
-----------------------------
By default ``fork`` is used so functions-under-test do not need to be
picklable by module reference.

Set the ``LMTTFY_TEST_SPAWN`` environment variable to any non-empty value
to use ``spawn`` instead.  This lets you test Windows-style multiprocessing
behaviour on Linux::

    LMTTFY_TEST_SPAWN=1 python -m pytest tests/

When ``spawn`` is active the ``src`` directory is also injected into
``PYTHONPATH`` so the child processes can import ``lmttfy`` and its
``_test_helpers`` module.
"""

import multiprocessing
import os
import sys
from pathlib import Path


# add the src directory to sys.path so that lmttfy can be imported
_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

# when testing with spawn (Windows compat mode), propagate the src directory
# to child processes via PYTHONPATH so they can import lmttfy.
if os.environ.get("LMTTFY_TEST_SPAWN"):
    os.environ.setdefault("PYTHONPATH", _src)
    multiprocessing.set_start_method("spawn", force=True)
    import warnings
    warnings.warn(
        "multiprocessing start method set to 'spawn' (Windows compat mode). "
        "Functions decorated with @invoke_in_sp must be importable by module path.",
        stacklevel=2,
    )
else:
    # use fork so that multiprocessing.Process inherits memory from the parent,
    # avoiding pickling issues for functions defined inside tests.
    #
    # force=True lets us override the forkserver/spawn default on Python 3.14+.
    multiprocessing.set_start_method("fork", force=True)
