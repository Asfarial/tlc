"""
Additional Utilities module
"""
from datetime import datetime
from typing import Callable


def exe_time(func: Callable) -> Callable:
    """
    Decorator to print
    Function Execution Time
    """
    def inner(*args, **kwargs):
        start_time = datetime.now()
        func(*args, **kwargs)
        print(datetime.now() - start_time)

    return inner
