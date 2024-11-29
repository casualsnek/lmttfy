from typing import TypeVar

CALL_STATE_INCOMPLETE: int = 0
CALL_STATE_SUCCESS: int = 1
CALL_STATE_ERROR: int = 2

R = TypeVar('R')  # Maybe @overload ????


def pass_(*args, **kwargs):
    """
    Does nothing at all
    """
    pass
