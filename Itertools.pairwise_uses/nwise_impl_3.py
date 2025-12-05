from itertools import pairwise

def nwise(iterable, n):
    if n == 2:
        yield from pairwise(iterable)
        return
    for (head, *_), tail in pairwise(nwise(iterable, n - 1)):
        yield (head, *tail)

print(list(nwise("ABCDE",3)))