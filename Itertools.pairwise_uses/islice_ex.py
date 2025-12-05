from itertools import islice

for i in islice(range(20), 5):
    print(i)

li = [2,4,5,7,8,10,20]
print(list(islice(li,1,6,2)))