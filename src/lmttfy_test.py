from lmttfy.thread import invoke_in_thread
from lmttfy.process import invoke_in_sp
from time import time, perf_counter, sleep

@invoke_in_sp()
def sleep_n_add(sleep_sec, a, b):
    sleep(sleep_sec)
    return a+b

x = sleep_n_add(3, 1, 2)
y = sleep_n_add(3, 2, 2)

print(x)
print(y)

print("x", x.wait(), time())
print("y", y.wait(), time())
