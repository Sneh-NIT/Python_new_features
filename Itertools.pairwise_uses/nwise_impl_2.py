from itertools import pairwise

def triplewise(iterable):
    for (a, _), (b, c) in pairwise(pairwise(iterable)):
        yield (a, b, c)

print(list(triplewise("ABCDE")))