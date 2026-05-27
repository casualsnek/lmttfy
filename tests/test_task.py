"""Tests for :class:`lmttfy.task.LMTTask` — the unified task wrapper."""

from typing import Any

import pytest

from lmttfy.task import LMTTask
from lmttfy.thread import ThreadedCall, invoke_in_thread
from lmttfy.process import MultiProcessedCall, invoke_in_sp
from lmttfy.exceptions import BurstWhileNoTaskErrorsException

from tests.helpers import sleep_n_add, raise_value_error


class TestLMTTaskWrapper:
    """LMTTask correctly delegates to the wrapped task object."""

    def test_wraps_threaded_call(self):
        """LMTTask wrapping a ThreadedCall returns the right result via wait()."""
        tc = ThreadedCall(sleep_n_add, id(sleep_n_add), 0.05, 10, 20)
        task = LMTTask(tc)
        assert task.wait() == 30
        assert isinstance(task._task, ThreadedCall)

    def test_wraps_multi_processed_call(self):
        """LMTTask wrapping a MultiProcessedCall returns the right result via wait()."""
        mp = MultiProcessedCall(sleep_n_add, id(sleep_n_add), False, 0.05, 100, 200)
        task = LMTTask(mp)
        assert task.wait() == 300
        assert isinstance(task._task, MultiProcessedCall)

    def test_burst_delegates(self):
        """LMTTask.burst() raises the captured exception from the inner task."""
        tc = ThreadedCall(raise_value_error, id(raise_value_error), "nope")
        task = LMTTask(tc)
        task.wait()
        with pytest.raises(ValueError, match="nope"):
            task.burst()

    def test_burst_before_error(self):
        """LMTTask.burst() raises BurstWhileNoTaskErrorsException when no error."""
        tc = ThreadedCall(sleep_n_add, id(sleep_n_add), 0.05, 1, 2)
        task = LMTTask(tc)
        task.wait()
        with pytest.raises(BurstWhileNoTaskErrorsException):
            task.burst()

    def test_on_complete_delegates(self):
        """LMTTask.on_complete() fires the callback via the inner task."""
        tc = ThreadedCall(sleep_n_add, id(sleep_n_add), 0.05, 1, 2)
        task = LMTTask(tc)
        collected: list[Any] = []
        task.on_complete(collected.append)
        task.wait()
        assert collected == [3]

    def test_on_error_delegates(self):
        """LMTTask.on_error() fires the callback via the inner task."""
        tc = ThreadedCall(raise_value_error, id(raise_value_error), "bad")
        task = LMTTask(tc)
        collected: list[Any] = []
        task.on_error(lambda e: collected.append(e))
        task.wait()
        assert len(collected) == 1

    def test_chaining(self):
        """LMTTask.on_complete() and on_error() return self for chaining."""
        tc = ThreadedCall(sleep_n_add, id(sleep_n_add), 0.05, 1, 2)
        task = LMTTask(tc)
        assert task.on_complete(print) is task
        assert task.on_error(print) is task

    def test_wait_timeout(self):
        """LMTTask.wait(timeout=…) delegates correctly."""
        tc = ThreadedCall(sleep_n_add, id(sleep_n_add), 2, 1, 2)
        task = LMTTask(tc)
        assert task.wait(timeout=0.01) is None  # still running
        assert task.wait() == 3  # now complete


class TestLMTTaskViaDecorator:
    """LMTTask is returned by invoke_in_thread and invoke_in_sp."""

    def test_invoke_in_thread_returns_lmttask(self):
        """invoke_in_thread() returns an LMTTask wrapping a ThreadedCall."""
        f = invoke_in_thread()(sleep_n_add)
        result = f(0.05, 1, 2)
        assert isinstance(result, LMTTask)
        assert isinstance(result._task, ThreadedCall)
        assert result.wait() == 3

    def test_invoke_in_sp_returns_lmttask(self):
        """invoke_in_sp() returns an LMTTask wrapping a MultiProcessedCall."""
        f = invoke_in_sp()(sleep_n_add)
        result = f(0.05, 1, 2)
        assert isinstance(result, LMTTask)
        assert isinstance(result._task, MultiProcessedCall)
        assert result.wait() == 3
