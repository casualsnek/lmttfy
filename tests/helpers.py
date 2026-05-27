"""Test helper functions for lmttfy."""

from time import sleep


def sleep_n_add(sleep_sec: float, a: float, b: float) -> float:
    """Sleep for *sleep_sec* seconds, then return ``a + b``."""
    sleep(sleep_sec)
    return a + b


def raise_value_error(msg: str = "boom") -> float:
    """Always raise :exc:`ValueError`."""
    raise ValueError(msg)
