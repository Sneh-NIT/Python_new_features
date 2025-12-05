from collections import deque
from itertools import islice

def my_pairwise(data):
    data = iter(data)
    window = deque(islice(data, 1), maxlen=2)
    for value in data:
        window.append(value)
        yield tuple(window)

print(my_pairwise([700,1000,800,750]))