from collections import deque
from itertools import islice

def nwise(iterable, n):
    iterable = iter(iterable)
    window = deque(islice(iterable, n-1), maxlen=n)
    for value in iterable:
        window.append(value)
        yield tuple(window)

print(nwise([700,1000,300,500],2))