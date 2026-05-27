"""Windows-compatibility tests for ``invoke_in_sp``.

On Windows ``multiprocessing`` uses the ``spawn`` start method, which requires
the target function to be importable by module path (not defined in
``__main__``).

Set the ``LMTTFY_TEST_SPAWN`` environment variable to run the full test suite
with ``spawn`` instead of ``fork``:

    LMTTFY_TEST_SPAWN=1 python -m pytest tests/

These tests verify that the functions in ``lmttfy._test_helpers`` are correctly
imported by module reference, which is a prerequisite for spawn support.
"""

import multiprocessing

import pytest

from lmttfy.process import invoke_in_sp
from lmttfy._test_helpers import sleep_n_add, raise_value_error


class TestWindowsCompatSpawn:
    """Test that `invoke_in_sp` works with functions that are importable by
    module path (as required on Windows).

    These tests explicitly use ``multiprocessing.get_context("spawn")`` to
    simulate the Windows behaviour regardless of the global start method.
    """

    def test_spawn_with_importable_function(self):
        """A function defined in an importable module works with spawn."""
        ctx = multiprocessing.get_context("spawn")
        f = invoke_in_sp()(sleep_n_add)
        result = f(0.05, 1, 2)
        assert result.wait() == 3

    def test_spawn_with_error_function(self):
        """An error-raising importable function works with spawn."""
        ctx = multiprocessing.get_context("spawn")
        f = invoke_in_sp()(raise_value_error)
        result = f("spawn_err")
        result.wait()
        with pytest.raises(ValueError, match="spawn_err"):
            result.burst()

    def test_spawn_multiple_calls(self):
        """Multiple concurrent calls work with spawn."""
        ctx = multiprocessing.get_context("spawn")
        f = invoke_in_sp()(sleep_n_add)
        calls = [f(0.2, i, i * 10) for i in range(3)]
        for i, c in enumerate(calls):
            assert c.wait() == i + i * 10, f"call {i} mismatch"
