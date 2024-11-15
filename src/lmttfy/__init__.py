from .exceptions import MaxConcurrentCallsLimitExceedException, BurstWhileNoTaskErrorsException
from typing import Callable
from functools import wraps
import threading
import multiprocessing
import time

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
        self.__fc_ret = None
        self.__state = [0, 0]
        self.__fid = fid
        self.__function = function
        self.__onComplete: Callable = self.__pass
        self.__onError: Callable = self.__pass
        self.__ipcq = multiprocessing.Queue()
        self.thread = threading.Thread(target=self.__exe, args=args, kwargs=kwargs)
        self.thread.daemon = False
        self.thread.start()

    def on_complete(self, func: Callable):
        """
        Sets the function which gets passed the return value from the threaded function after it's done
        """
        self.__onComplete = func
        if self.__state[0] != 0:
            self.__onComplete(self)
        return self

    def on_error(self, func: Callable):
        """
        Sets the function which will be called when the function call in the thread raises any unhandled exceptions
        """
        self.__onError = func
        if self.__state[1] != 0:
            self.__onError(self)
        return self

    def burst(self):
        """
        Raises the captured exception during execution of the function in different thread
        """
        raise self.__exception

    def wait(self):
        while self.__state[0] != 1:
            time.sleep(0.01)
        return self.__fc_ret

    def __exe(self, *args, **kwargs):
        """
        This method gets threaded and called to execute the actual function
        """
        # self.__fc_ret = None
        # self.__state = [0, 0]
        try:
            self.__fc_ret = self.__function(*args, **kwargs)
            self.__state[0] = 1
            self.__onComplete(self.__fc_ret)
        except Exception as e:
            self.__exception = e
            self.__state[1] = 1
            self.__onError(self)
        with FUN_CALL_COUNTER_LOCK:
            if FUN_CALL_COUNTER[self.__fid] > 0:
                FUN_CALL_COUNTER[self.__fid] = FUN_CALL_COUNTER[self.__fid] - 1

    @staticmethod
    def __pass(*args, **kwargs):
        pass

# Multiprocessing related global variables
PROCESS_CALL_COUNTER_LOCK = multiprocessing.Lock()
PROCESS_CALL_COUNTER: dict[int, int] = {}

class MultiProcessedCall:
    """
    A class representing a multiprocessing call, used to add error handler and completion handler
    or raise the Exception that occurred in the process to handle it properly
    """

    def __init__(self, function: Callable, fid: int, terminate_on_return: bool=False, *args, **kwargs):
        """
        Initializes the MultiProcessedCall object
        """
        self.__exception: Exception = BurstWhileNoTaskErrorsException("Burst called with no error on multiprocessing task")
        self.__fid: int = 0
        self.__fc_ret = None
        self.__state = [0, 0]
        self.__fid = fid
        self.__function = function
        self.__tor = terminate_on_return
        self.__onComplete: Callable = self.__pass
        self.__onError: Callable = self.__pass
        self.process = multiprocessing.Process(target=self.__exe, args=args, kwargs=kwargs)
        self.process.start()

    def on_complete(self, func: Callable):
        """
        Sets the function which gets passed the return value from the multiprocessing function after it's done
        """
        self.__onComplete = func
        if self.__state[0] != 0:
            self.__onComplete(self)
        return self

    def on_error(self, func: Callable):
        """
        Sets the function which will be called when the function call in the process raises any unhandled exceptions
        """
        self.__onError = func
        if self.__state[1] != 0:
            self.__onError(self)
        return self

    def burst(self):
        """
        Raises the captured exception during execution of the function in a different process
        """
        raise self.__exception

    def wait(self):
        print("Wait: Current state", self.__state)
        while self.__state[0] != 1:
            time.sleep(0.01)
            print("Wait: Current state", self.__state)
        return self.__fc_ret

    def __exe(self, *args, **kwargs):
        """
        This method gets processed and called to execute the actual function
        """
        try:
            self.__fc_ret = self.__function(*args, **kwargs)
            self.__state[0] = 1
            print("Exec, state updated", self.__state)
            if self.__tor and self.process.is_alive():
                self.process.terminate()
            self.__onComplete(self.__fc_ret)
        except Exception as e:
            self.__exception = e
            self.__state[1] = 1
            self.__onError(self)
        with PROCESS_CALL_COUNTER_LOCK:
            if PROCESS_CALL_COUNTER[self.__fid] > 0:
                PROCESS_CALL_COUNTER[self.__fid] -= 1

    def __sync_state(self) -> None:
        """
        Sync the state variable, and allow actions across between sub-process and current.
        :return: None
        """
        # TODO: Implement

        pass

    @staticmethod
    def __pass(*args, **kwargs):
        pass


def invoke_in_thread(max_concurrent_execs=-1):
    """
    Function decorator to return an instance of ThreadedCall when a function is called
    """
    def decorator(function):
        @wraps(function)
        def wrapper(*args, **kwargs):
            fid = id(function)
            with FUN_CALL_COUNTER_LOCK:
                if fid in FUN_CALL_COUNTER:
                    if ( FUN_CALL_COUNTER[fid] >= max_concurrent_execs ) and max_concurrent_execs != -1 :
                        raise MaxConcurrentCallsLimitExceedException(f"Already running ! Threaded function {str(function)} only allows {max_concurrent_execs} concurrent executions !")
                    else:
                        FUN_CALL_COUNTER[fid] = FUN_CALL_COUNTER[fid] + 1
                else:
                    FUN_CALL_COUNTER[fid] = 1
            return ThreadedCall(function, fid, *args, **kwargs)
        return wrapper
    return decorator


def invoke_in_sp(max_concurrent_execs=-1, terminate_on_return=False):
    """
    Function decorator that turns any function into a multiprocessing one with ease
    :param max_concurrent_execs: Number of parallel executions allowed for this function
    :param terminate_on_return: Terminates the sub-process after it returns, the threads spawned and everything will be terminated
    """
    def decorator(function):
        @wraps(function)
        def wrapper(*args, **kwargs):
            fid = id(function)
            with PROCESS_CALL_COUNTER_LOCK:
                if fid in PROCESS_CALL_COUNTER:
                    if (PROCESS_CALL_COUNTER[fid] >= max_concurrent_execs) and max_concurrent_execs != -1:
                        raise MaxConcurrentCallsLimitExceedException(f"Already running! Multiprocessing function {str(function)} only allows {max_concurrent_execs} concurrent executions!")
                    else:
                        PROCESS_CALL_COUNTER[fid] += 1
                else:
                    PROCESS_CALL_COUNTER[fid] = 1
            return MultiProcessedCall(function, fid, terminate_on_return, *args, **kwargs)
        return wrapper
    return decorator
