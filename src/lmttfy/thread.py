from .common import CALL_STATE_INCOMPLETE, CALL_STATE_SUCCESS, CALL_STATE_ERROR, R, pass_
from .exceptions import MaxConcurrentCallsLimitExceedException, BurstWhileNoTaskErrorsException
from typing import Callable, Any
from functools import wraps
import logging
import threading

# Threading related global variables
FUN_CALL_COUNTER_LOCK = threading.Lock()
FUN_CALL_COUNTER: dict[int, int] = {}


class ThreadedCall:
    """
    A class representing a threaded call, used to add error handler and completion handler
    or raise the Exception that occurred in the thread to handle it properly
    """

    def __init__(self, function, fid, *args, **kwargs):
        """
        Initializes the ThreadedCall object
        """
        self.__exception: Exception = BurstWhileNoTaskErrorsException("Burst called with no error on threaded task")
        self.__fid: int = 0
        self.__fc_ret: object = None
        self.__state: int = CALL_STATE_INCOMPLETE
        self.__fid: int = fid
        self.__function: Callable = function
        self.__onComplete: Callable[[Any], None] = pass_
        self.__onError: Callable[[Any], None] = pass_
        self.thread: threading.Thread = threading.Thread(target=self.__exe, args=args, kwargs=kwargs)
        self.thread.daemon = True
        self.thread.start()
        logging.info('The function was executed in a separate thread: fid=%s', self.__fid)

    def on_complete(self, func: Callable, immediate_callback_if_done: bool = True):
        """
        Sets the function which gets passed the return value from the threaded function after it's done
        """
        self.__onComplete = func
        if self.__state == CALL_STATE_SUCCESS and immediate_callback_if_done:
            self.__onComplete(self.__fc_ret)
        return self

    def on_error(self, func: Callable, immediate_callback_if_done: bool = True):
        """
        Sets the function which will be called when the function call in the thread raises any unhandled exceptions.
        """
        self.__onError = func
        if self.__state == CALL_STATE_ERROR and immediate_callback_if_done:
            self.__onError(self.__exception)
        return self

    def burst(self):
        """
        Raises the captured exception during execution of the function in different thread
        """
        raise self.__exception

    def wait(self, timeout: int = 0) -> Any:
        """
        Wait for the wrapped function to return or error-out
        """
        if self.__state == CALL_STATE_INCOMPLETE:
            # Return or error is set when the wrapped function finishes execution, and thread dies at that point
            if timeout > 0:
                self.thread.join(timeout=timeout)
            else:
                self.thread.join()
        return self.__fc_ret

    def __exe(self, *args, **kwargs) -> None:
        """
        This method gets threaded and called to execute the actual function
        """
        # self.__fc_ret = None
        # self.__state = [0, 0]
        try:
            self.__fc_ret = self.__function(*args, **kwargs)
            logging.info("The internal func in thread returned successfully: fid=%s", self.__fid)
            self.__state = CALL_STATE_SUCCESS
            self.__onComplete(self.__fc_ret)
        except Exception as e:
            logging.info("The internal func in thread errored out: fid=%s", self.__fid)
            self.__exception = e
            self.__state = CALL_STATE_ERROR
            self.__onError(self)
        with FUN_CALL_COUNTER_LOCK:
            if FUN_CALL_COUNTER[self.__fid] > 0:
                logging.info("Decrementing thread call counter: fid=%s", self.__fid)
                FUN_CALL_COUNTER[self.__fid] = FUN_CALL_COUNTER[self.__fid] - 1


def invoke_in_thread(max_concurrent_execs=-1):
    """
    Function decorator to return an instance of ThreadedCall when a function is called
    """

    def decorator(function: Callable[..., R]) -> Callable[..., ThreadedCall]:
        @wraps(function)
        def wrapper(*args, **kwargs):
            fid = id(function)
            with FUN_CALL_COUNTER_LOCK:
                if fid in FUN_CALL_COUNTER:
                    if (FUN_CALL_COUNTER[fid] >= max_concurrent_execs) and max_concurrent_execs != -1:
                        raise MaxConcurrentCallsLimitExceedException(
                            f"Already running ! Threaded function {str(function)} only allows {max_concurrent_execs} "
                            f"concurrent executions !")
                    else:
                        FUN_CALL_COUNTER[fid] = FUN_CALL_COUNTER[fid] + 1
                else:
                    FUN_CALL_COUNTER[fid] = 1
            return ThreadedCall(function, fid, *args, **kwargs)

        return wrapper

    return decorator
