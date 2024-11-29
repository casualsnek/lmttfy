from queue import Empty
from .exceptions import MaxConcurrentCallsLimitExceedException, BurstWhileNoTaskErrorsException
from .common import CALL_STATE_INCOMPLETE, CALL_STATE_SUCCESS, CALL_STATE_ERROR, R, pass_
from typing import Callable, Any, Tuple
from functools import wraps
import os
import signal
import subprocess
import platform
import multiprocessing
import threading
import logging

# Multiprocessing related global variables
PROCESS_CALL_COUNTER_LOCK = multiprocessing.Lock()
PROCESS_CALL_COUNTER: dict[int, int] = {}


def mp_exec_wrapper(function: Callable[..., Any], queue: multiprocessing.Queue, *args, **kwargs) -> None:
    """
            This method gets processed and called to execute the actual function
            """
    try:
        fc_ret = function(*args, **kwargs)
        queue.put(("success_set", fc_ret))
    except Exception as e:
        queue.put(("exception_set", e))


class MultiProcessedCall:
    """
    A class representing a multiprocessing call, used to add error handler and completion handler
    or raise the Exception that occurred in the process to handle it properly
    """

    def __init__(self, function: Callable, fid: int, terminate_on_return: bool = False, *args, **kwargs):
        """
        Initializes the MultiProcessedCall object
        """
        self.__exception: Exception = BurstWhileNoTaskErrorsException(
            "Burst called with no error on multiprocessing task")
        self.__fid: int = 0
        self.__fc_ret = None
        self.__state = CALL_STATE_INCOMPLETE
        self.__fid = fid
        self.__tor = terminate_on_return
        self.__ipcq = multiprocessing.Queue()
        self.__onComplete: Callable[[Any], None] = pass_
        self.__onError: Callable[[Any], None] = pass_
        self.__sync_thread = threading.Thread(target=self.__sync_state)
        self.__sync_thread.daemon = True
        self.__sync_thread.start()
        logging.info("Inter process syncing thread started for fid: %s", self.__fid)
        self.process = multiprocessing.Process(target=mp_exec_wrapper, args=(function, self.__ipcq,) + args, kwargs=kwargs)
        self.process.daemon = True
        self.process.start()
        logging.info('Sub-process started for fid: %s', self.__fid)

    def on_complete(self, func: Callable, immediate_callback_if_done: bool = True):
        """
        Sets the function which gets passed the return value from the multiprocessing function after it's done
        """
        self.__onComplete = func
        if self.__state == CALL_STATE_SUCCESS and immediate_callback_if_done:
            self.__onComplete(self.__fc_ret)
        return self

    def on_error(self, func: Callable, immediate_callback_if_done: bool = True):
        """
        Sets the function which will be called when the function call in the process raises any unhandled exceptions
        """
        self.__onError = func
        if self.__state == CALL_STATE_ERROR and immediate_callback_if_done:
            self.__onError(self.__exception)
        return self

    def burst(self):
        """
        Raises the captured exception during execution of the function in a different process
        """
        raise self.__exception

    def wait(self, timeout: int = 0) -> Any:
        """
        Wait for the wrapped function to return or error-out
        """
        if self.__state == CALL_STATE_INCOMPLETE:
            # Sync thread terminates once we get error or return value
            if timeout > 0:
                self.__sync_thread.join(timeout=timeout)
            else:
                self.__sync_thread.join()
        return self.__fc_ret

    def __sync_state(self) -> None:
        """
        Sync the state variable, and allow actions across between sub-process and current.
        :return: None
        """
        while self.__state == CALL_STATE_INCOMPLETE:
            try:
                cmd: Tuple[str, str | object] = self.__ipcq.get(timeout=0.1)
                if cmd[0] == 'success_set':
                    logging.info("The sub-process returned successfully: fid=%s", self.__fid)
                    self.__state = CALL_STATE_SUCCESS
                    self.__fc_ret = cmd[1]
                if cmd[0] == 'exception_set':
                    logging.info("The sub-process errored out and set an exception: fid=%s", self.__fid)
                    self.__state = CALL_STATE_ERROR
                    self.__exception = cmd[1]
                if cmd[0] in ['success_set', 'exception_set']:
                    # The process returned or errored out
                    # Kill the process if required
                    # The process may not die if it's stuck doing I/O stuff, handle it properly
                    if self.__tor and self.process.is_alive():
                        self.process.terminate()
                        self.process.join(timeout=5)
                        if self.process.is_alive():
                            logging.error("Pesky sub-process is still alive, attempting to kill: fid=%s", self.__fid)
                            # Pesky process is still alive
                            # If on linux/nix call os.kill with process.pid, and for windows handle differently
                            if platform.system() == "Windows":
                                subprocess.run(["taskkill", "/PID", str(self.process.pid), "/F"], check=True)
                            else:
                                os.kill(self.process.pid, signal.SIGKILL)
                    # Run on complete callbacks
                    if self.__state == CALL_STATE_SUCCESS:
                        self.__onComplete(self.__fc_ret)
                    elif self.__state == CALL_STATE_ERROR:
                        self.__onError(self.__exception)
                    # Clear the counter
                    with PROCESS_CALL_COUNTER_LOCK:
                        if PROCESS_CALL_COUNTER[self.__fid] > 0:
                            logging.info("Decrementing process call counter: fid=%s", self.__fid)
                            PROCESS_CALL_COUNTER[self.__fid] -= 1
            except (TimeoutError, Empty):
                logging.warning("Timeout occurred in sync state thread for MP: fid=%s", self.__fid)


def invoke_in_sp(max_concurrent_execs=-1, terminate_on_return=False):
    """
    Function decorator that turns any function into a multiprocessing one with ease
    :param max_concurrent_execs: Number of parallel executions allowed for this function
    :param terminate_on_return: Kill the sub-process after it returns, sub threads and everything will be terminated
    """

    def decorator(function: Callable[..., R]) -> Callable[..., MultiProcessedCall]:
        @wraps(function)
        def wrapper(*args, **kwargs):
            fid = id(function)
            with PROCESS_CALL_COUNTER_LOCK:
                if fid in PROCESS_CALL_COUNTER:
                    if (PROCESS_CALL_COUNTER[fid] >= max_concurrent_execs) and max_concurrent_execs != -1:
                        raise MaxConcurrentCallsLimitExceedException(
                            f"Already running! Multiprocessing function {str(function)} only allows "
                            f"{max_concurrent_execs} concurrent executions!")
                    else:
                        PROCESS_CALL_COUNTER[fid] += 1
                else:
                    PROCESS_CALL_COUNTER[fid] = 1
            return MultiProcessedCall(function, fid, terminate_on_return, *args, **kwargs)

        return wrapper

    return decorator
