
a = "tzzzreea"

# from collections import Counter
# freq = Counter(a)
# print(freq)
# #primary sort - largest to smallest. 
# #secondary sort - x[0] smallest to largest 
# b = sorted(freq.items(), key = lambda x : (-x[1], x[0]))
# print(b)

# c = sorted(freq.items(), key = lambda x : (x[1], x[0]))
# print(c)

# print(*(val for val, f in freq.items()))

# v = [[5,'a'],[4,'e'],[6,'c']]
# print(v)
# v.sort(key = lambda x : x[1])

# print(v)



# from collections import Counter

# s = "helloaaaa"

# count = Counter(s)

# print(count)

# for key in count:
#     print(f"{key=}")
    
# for key, val in count.items():
#     print(f"{key=}, {val=}")

# count = sorted(count.items(), key = lambda x: x[1])
# print(count)

# del count['o']

# print(count)



# records = [[40,"Bob"], [30, "Din"], [40,"Alex"]]

# print(records)

# records.sort()
# print(records)

# d = {1: 'D', 2: 'B', 4: 'B', 4: 'E', 5: 'A'}
# print(sorted(d.items(), key=lambda item: item[0]))

# (First Item, Second Parameter)
data = [
    ('Apple', 10),
    ('Banana', 5),
    ('Apple', 20),
    ('Orange', 15),
    ('Banana', 8)
]

# 2025-12-05 09:35:17 PST

#if i want to sort in this particular way: sort first item smallest to largest.
#  If first item is equal, sort second item by largest to smallest.
#[('Apple', 20), ('Apple', 10), ('Banana', 8), ('Banana', 5), ('Orange', 15)]
# print(sorted(data, key=lambda item: (item[0], -item[1])))

#sort by 2nd item (smallest to largest)
#[('Banana', 5), ('Banana', 8), ('Apple', 10), ('Orange', 15), ('Apple', 20)]
# print(sorted(data, key=lambda item: item[1]))

# data = [[40,"Bob"], [30, "Din"], [40,"Alex"], [50,"Carla"]]
# #30 Din, 40 Bob, 40 alex, 50 Carla
# first_sort = sorted(data, key = lambda item: item[1], reverse=True)
# print(f"{first_sort=}")

# second_sort = sorted(first_sort, key = lambda item: item[0])
# print(f"{second_sort=}")

# wrong_sort = sorted(first_sort)
# print(f"{wrong_sort=}")


#print(sorted(data, key=lambda item: (item[0], -item[1]))) #error

# reverse sort by string first, and then another sort by the number
first_sort = sorted(data, reverse=True, key=lambda item: item[1])
#Din, Bob, Alex
# print(first_sort)


# sorted(first_sort, key=lambda item: item[0])
# Effect: Primary Sort Only (Maintains Stability)
# This option tells sorted() to look only at the element at index 0 (the number, e.g., 30 or 40) for comparison.

# It performs a simple ascending sort based on the numbers.

# Crucially, since Python's sort is stable, when two items have the same primary key (e.g., the two items with 40), their relative order from the first_sort step is preserved.
second_sort= sorted(first_sort, key=lambda item: item[0])
# print(second_sort)


# Effect: Sorts All Items Based on Tuple Comparison
# When you pass an iterable of tuples or lists (like first_sort) to sorted() without a key, Python defaults to comparing the items directly.

# Python compares tuples/lists element by element, from left to right:

# It compares item[0]. If they are different, the sort is done.

# If item[0] is equal, it proceeds to compare item[1].
second_sort= sorted(first_sort)
# print(second_sort)

#note stable sorting order. casefold is stricter than lower since it accounts for unicode
# also note, it's not key = str.casefold() or str.lower()
print(sorted("This is Andrew anDrew test string from ANDREW".split(), key = str.casefold))
print(sorted("This is Andrew anDrew test string from ANDREW".split(), key = str.lower))

