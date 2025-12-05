from itertools import pairwise

balance_history = [700,1000,800,750]

for before, after in pairwise(balance_history):
    change = after - before
    print(f"Balance changed by {change:+}.")