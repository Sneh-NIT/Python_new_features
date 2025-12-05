from collections import deque
from itertools import islice, tee

def nwise(iterable, n):
    iterators = tee(iter(iterable, n))
    for idx, iterator in enumerate(iterators):
        for _ in islice(iterator, idx):
            pass

    yield from zip(*iterators)



print(next(nwise([700,1000,300,500],3)))