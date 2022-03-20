from datetime import datetime


def exe_time(func):
    def inner(*args, **kwargs):
        start_time = datetime.now()
        func(*args, **kwargs)
        print(datetime.now() - start_time)
    return inner