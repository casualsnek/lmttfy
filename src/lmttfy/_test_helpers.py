"""Test helper functions that are importable by module path.

This module lives inside the ``lmttfy`` package so that functions can be
pickled by module reference — required by ``multiprocessing`` on platforms
that use the ``spawn`` start method (e.g. Windows).
"""

from time import sleep


def sleep_n_add(sleep_sec: float, a: float, b: float) -> float:
    """Sleep for *sleep_sec* seconds, then return ``a + b``."""
    sleep(sleep_sec)
    return a + b


def raise_value_error(msg: str = "boom") -> float:
    """Always raise :exc:`ValueError`."""
    raise ValueError(msg)
