# def swap_case(s):
#     sb = ''
#     for c in s:
#         if c.islower():
#             sb+=(c.upper())
#         elif c.isupper():
#             sb+=(c.lower())
#         else:
#             sb.append(c)
#     return sb  # Join the list into a string and return it

# # This function takes a string and swaps the case of each character.
# if __name__ == '__main__':
#     s = input()
#     result = swap_case(s)
#     print(result)


# s = "ABCeD"
# t = "CD"

# if t in s:
#     print("Yes")


# def count_substring(string, sub_string):
#     count = 0
#     for i in range(len(string)):
#         if sub_string in string[i:i+len(sub_string)]:
#             count+=1
#     return count

# if __name__ == '__main__':
#     string = input().strip()
#     sub_string = input().strip()
    
#     count = count_substring(string, sub_string)
#     print(count)

# s = "AQ2"

# isalnum = isalpha = isdigit = islower = isupper = False

# for x in s:
#     if x.isalnum():
#         isalnum = True
#     if x.isalpha():
#         isalpha = True
#     if x.isdigit():
#         isdigit = True
#     if x.islower():
#         islower = True
#     if x.isupper():
#         isupper = True
    
# print(isalnum)
# print(isalpha)
# print(isdigit)
# print(islower)
# print(isupper)



# import textwrap

# s = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat."

# print(textwrap.wrap(s, 3))
# print(textwrap.fill(s, 3))

# def print_formatted(number):
#     width = len(bin(number)) -2 # Necessary to calculate width of the binary representation

#     for i in range(1, number + 1):
#         print(f"{i:{width}d} {i:{width}o} {i:{width}X} {i:{width}b}")
        # decimal = str(i).rjust(width)
        # oct1 =  oct(i)[2:].rjust(width)
        # hex1 = hex(i)[2:].upper().rjust(width)
        # bin1 = bin(i)[2:].rjust(width)
        # print(f"{decimal} {oct1} {hex1} {bin1}")

        
# print(bin(17))
# print(oct(17)[2:])
# print(hex(17)[2:].rjust(10,'-'))
# print(hex)
#print_formatted(17)


# s = "12abc,,,,4ll  smith"

# print(s.split())
# print(s.split(","))


# def solve(s):
#     s = s.split()
#     return " ".join(word.capitalize() for word in s)


# def solve(s): 
#     lis=s.split(" ") 
#     word=[i.capitalize() for i in lis] 
#     return ' '.join(word)

from collections import defaultdict
# # def minion_game(string):
#     # your code goes here
#     s_substr = defaultdict(int)
#     k_substr = defaultdict(int)
#     #k_substr = {}
#     for i in range(len(string)):
#         for j in range(i, len(string)):
#             s = string[i:j+1]
#             #print(s)
#             if s[0] in "AEIOU":
#                 k_substr[s]+=1
#             else:
#                 s_substr[s]+=1
#     s_sum, k_sum=0, 0
#     for s, count in s_substr.items():
#         print(f"{s=}, {count=}")
#         s_sum+=count
#     for k, count in k_substr.items():
#         k_sum+=count
#     if s_sum > k_sum:
#         return f"Stuart {s_sum}"
#     elif s_sum < k_sum:
#         return f"Kevin {k_sum}"
#     return "Draw"


# print(minion_game("BANANA"))



# s = "123456789"
# # ["1234","5678","9***"]
# k = 4
# print(len(s)//k)


# res = []

# #handle full chunks
# for i in range(len(s)//k):
#     index = i*k # 0,4,8, etc
#     res.append(s[index:index+k]) # size k

# #handle remainder
# r = len(s) % k # 9 % 4 = 1
# if r:
#     idx = len(s) - r # 9 - 1 = 8. we start from s[8] = 9
#     padding = '*' * (k - r)
#     res.append(s[idx:] + padding) # one `9` and three `*`

# print(res)

# res=[]


# x = []

# for i in range(0,len(s),k):
#     chunk = s[i:i+k]
#     if len(chunk) < k:
#         chunk+= (k-len(chunk) )*'*'
#     x.append(chunk)
# print(f"{x=}") 



# from collections import Counter
# s = "leetcode"
# mp = Counter([c for c in s])
# mp2 = Counter(s)
# print(mp)
# print(mp2)

# records.sort(key=lambda x:x[1], reverse=True)
# print(records)
# records.sort(key=lambda x:x[1])
# print(records)

# ans = []
# if not ans or ans[1] == 3:
#     print("OK")

#records = [[40,"Harry"], [40, "Berry"], [40,"Alex"]]
# records.sort()
# smallest = records[0][0]
# ans=[]
# second_lowest = -1
# for i in range(1,len(records)):
#     if records[i][0] == smallest:
#         continue
#     elif not ans:
#         second_lowest = records[i][0]
#         ans.append(records[i][1]) #add name
#     elif second_lowest == records[i][0]:
#         ans.append(records[i][1]) #add name
#     else:
#         break
# print(ans)
records = [['Harry', 37.21], ['Berry', 37.21], ['Tina', 37.2], ['Akriti', 41], ['Harsh', 39]]
# scores = (set(score for name, score in records))
# print(f"{scores=}")
# records.sort(key=lambda x:x[1])
# print(records)
# smallest = records[0][1]
# ans=[]
# second_lowest = -1
# for i in range(1,len(records)):
#     if records[i][1] == smallest:
#         continue
#     elif not ans:
#         second_lowest = records[i][1]
#         ans.append(records[i][0]) #add name
#     elif second_lowest == records[i][1]:
#         ans.append(records[i][0]) #add name
#     else:
#         break
# ans.sort()
# for name in ans:
#     print(name)



#use dict

#scores = {}
# from collections import defaultdict
# scores = defaultdict(list)
# for name, score in records:
#     scores[score].append(name)

# second_lowest = sorted(scores.keys())[1]

# for name in sorted(scores[second_lowest]):
#     print(name)

# if __name__ == '__main__':
#     input = [
#         "Krishna 67 68 69",
#         "Arjun 70 98 63",
#         "Malika 52 56 60"
#     ]
#     n = len(input)
#     student_marks = {}
#     for i in range(n):
#         name, *line = input[i].split()
        
#         print(*line)
        
#         print(f"{line=}")
#         scores = list(map(float, line))
#         print(scores)
#         student_marks[name] = scores

# from itertools import combinations

# count=0
# print(list(combinations(['a', 'a', 'c', 'd'], 2)))
# combs = list(combinations(['a', 'a', 'c', 'd'], 2))
# for comb in combs:
#     if 'a' in comb:
#         count+=1
# print(count/len(combs))

s = "34 21"
t  = """7 6429964 4173738 9941618 2744666 5392018 5813128 9452095
7 6517823 4135421 6418713 9924958 9370532 7940650 2027017
7 1506500 3460933 1550284 3679489 4538773 5216621 5645660
7 7443563 5181142 8804416 8726696 5358847 7155276 4433125
7 2230555 3920370 7851992 1176871 610460 309961 3921536
7 8518829 8639441 3373630 5036651 5291213 2308694 7477960
7 7178097 249343 9504976 8684596 6226627 1055259 4880436"""

t = """2 5 4
3 7 8 9 
5 5 7 8 9 10"""
# print(s.split())

# K,M = map(int, s.split())
# K,M = s.split() # K,M are both strings here
# print(type(K))
# print(K)
# print(M)

# nums = list(map(int,s.split()))
# print(nums)

# x = "      spacious   "
# print("1. All stripped: |"+ x.strip() +"|")
# print("2. left stripped: |"+ x.lstrip() +"|")
# print("3. right stripped: |"+ x.rstrip() +"|")


# data = []
# for line in t.strip().split('\n'):
#     nums = list(map(int, line.split()))
#     count = nums[0]
#     values = nums[1:]
#     assert len(values) == count  # Optional: sanity check
#     data.append(values)
# print(f"{data=}")
# print(type(data))

# 2025-12-05 12:38:07 PST

# data = [[1, 2], [3, 4]]

# import itertools

# # Using *data unpacks the list of lists into separate arguments for product
# # This is equivalent to calling: product([1, 2], [3, 4])
# print("Using product(*data):")
# for x in itertools.product(*data):
#     print(x)

# Now, if you try to put this into a set:
# print("\nSet of product(*data):")
# data_set = set(itertools.product([1, 2], [3, 4]))
# print(data_set)
# #note unpacking here for remaining elements.
# for a,b, *rest in data_set:
#     print("with *rest", a,b)
# for a,b in data_set:
#     print(a,b)

# print(set(itertools.product(*data)))

# print("\nWrong way:") #error
# print(set(itertools.product([[1, 2], [3, 4]])))

x = "abc"
y = "12"

for a in zip(x,y):
    print(a) #(a,1), (b,2)
print(zip(x,y))


# input = "3 5"

# input = input.split()
# print(input)
# N = int(input[0])
# X = int(input[1])

# print(N+1)
# print(X+1)
# input = list(map(int, input))
# print(input)

# arr = [ 0 for _ in range(5)]
# #print(arr)

# scores = [[89.0, 90.0, 78.0, 93.0, 80.0], [90.0, 91.0, 85.0, 88.0, 86.0], [91.0, 92.0, 83.0, 89.0, 90.5]]

# for score in zip(*scores):
#     print(sum(score))


s = "Sorting1234"
#s = "1qaz2wsx3edc4rfv5tgb6yhn7ujm8ik9ol0pQWERTYUIOPASDFGHJKLZXCVBNM"
# dumb way to do this

# lower = []
# upper = []
# odd = []
# even = []
# for ch in s:
#     if ch.islower():
#         lower.append(ch)
#     elif ch.isupper():
#         upper.append(ch)
#     elif ch.isdigit():
#         if int(ch) % 2:
#             odd.append(ch)
#         else:
#             even.append(ch)
#     else:
#         continue
#         #throw error
# lower.sort()
# upper.sort()
# odd.sort()
# even.sort()

# s =  lower + upper + odd + even
# print(''.join(s))


# lower = sorted([c for c in s if c.islower()])
# upper = sorted([c for c in s if c.isupper()])
# odd = sorted([c for c in s if c.isdigit() and int(c) % 2 == 1])
# even = sorted([c for c in s if c.isdigit() and int(c) % 2 == 0])

# s =  lower + upper + odd + even
# print(s)
# print(''.join(s))

# s = "Torting1234"
# result = ''.join(sorted(s, key=lambda c: (c.isdigit() == False, c.islower() == False, c)))
# print(result)  # Output: 1234ginortT


# numbers = [3, 1, 4, 1, 5, 9, 2, 6]

# sort even then odd

###
# Why False Sorts Before True (and why it's not "flipped" for the computer)
# Python's sorted() function (and list.sort()) performs an ascending sort by default. This means:

# Numbers: 0 comes before 1, 1 before 2, etc.
# Booleans: Since False evaluates to 0 and True evaluates to 1, False inherently sorts before True.
# ###

# def custom_sort(x):
#     return (int(x) % 2 ==1, x)

# #numbers.sort(key=lambda x: (x % 2 == 1, x) )
# numbers.sort(key=custom_sort)
# print(numbers)

# # so the secondary sort is basically any remaining number that's not even?
# Not quite. It's more precise to say:

# The secondary sort (x) is applied to any numbers that have the same value for the primary sort key.

# Let's break it down again with your example: numbers = [3, 1, 4, 1, 5, 9, 2, 6] and key=lambda x: (x % 2 != 0, x).

# When Python sorts:

# It calculates the key tuple for each number:

# 3: (True, 3)
# 1: (True, 1)
# 4: (False, 4)
# 1: (True, 1)
# 5: (True, 5)
# 9: (True, 9)
# 2: (False, 2)
# 6: (False, 6)
# Primary Sort: Python first compares the first element of each tuple (x % 2 != 0).

# All tuples starting with False (the even numbers) are grouped together and come first:
# (False, 4)
# (False, 2)
# (False, 6)
# All tuples starting with True (the odd numbers) are grouped together and come second:
# (True, 3)
# (True, 1)
# (True, 1)
# (True, 5)
# (True, 9)
# Secondary Sort: Only within these groups where the primary key is the same, Python then looks at the second element of the tuple (x).

# Within the "Even" group (where primary key is False):

# (False, 4)
# (False, 2)
# (False, 6) These are sorted by their second element (4, 2, 6), resulting in:
# (False, 2)
# (False, 4)
# (False, 6) So, the even numbers will be 2, 4, 6.
# Within the "Odd" group (where primary key is True):

# (True, 3)
# (True, 1)
# (True, 1)
# (True, 5)
# (True, 9) These are sorted by their second element (3, 1, 1, 5, 9), resulting in:
# (True, 1)
# (True, 1)
# (True, 3)
# (True, 5)
# (True, 9) So, the odd numbers will be 1, 1, 3, 5, 9.

# res=[5]
# actions = {"insert": lambda parts : res.insert(int(parts[1]),int(parts[2])), 
#            "remove": lambda parts : res.remove(int(parts[1])),
#            "append": lambda parts : res.append(int(parts[1])),
#            "sort": lambda parts: res.sort(),
#            "pop": lambda parts: res.pop(),
#            "reverse": lambda parts: res.reverse(),
#            "print" : lambda parts: print(res)
#            }



# x ="insert 0 5".split()
# x = ["print"]
# word = x[0]
# if word == "insert": # or len(x) == 3
#     index = int(x[1])
#     element = int(x[2])
#     actions[word](x)
# import numpy
# A = [3,5,7]
# B = [1,2,3]
# C=[x for x in A]
# print(type(C))
# print((C))
# A = numpy.array(A,float)
# B = numpy.array(B,float)

# print(numpy.add(A,B))

# A = {1,3,5}
# B = {2,4,6,5}

# print(B.intersection(A))

# print(B-A)

# for x in C2:
#     print(x)

#<generator object <genexpr> at 0x...>
# Unpack the generator expression into separate arguments for print()
#print(*(x for x in C2), sep='\n')

my_set={1, 2, 3, 4, 5, 6, 7, 8, 9}
# actions = {
#     "pop": lambda : my_set.pop(),
#     "remove": lambda val : my_set.remove(val),
#     "discard": lambda val : my_set.discard(val)
# }

# print(actions["remove"](9))
# print(actions["discard"](9))

# print(my_set)
# x = "20 11 12"

# #my_set.update([int(A) for A in x.split()])
# my_set.update(map(int,x.split()))
# print(my_set)

# s = {(0, 'R'), (1, 'a'), (2, 'n'), (5, 'x'), 'other_value'}
# result = s.difference(enumerate(['R', 'a', 'n', 'k']))
# print(result)


# A = [1,3,5,7]
# B = [3,5,7, 8]

# A=set(A)
# A |= set([6])
# print(A)

# from collections import Counter
# from typing import List
# class Solution:
#     def countCharacters(self, words: List[str], chars: str) -> int:
#         def create_map(s: str):
#             return Counter(s)
#         counter = create_map(chars)
#         total = 0
#         for word in words:
#             word_cnt = create_map(word)
#             isMissing = False
#             for ch in word_cnt:
#                 if word_cnt[ch] > counter[ch]:
#                     isMissing = True
#                     break
#             if isMissing == False:
#                 total+=len(word)
#         return total


# s = "HHackkker"

# from collections import Counter
# c = Counter(s)

# print(c.keys())
# print(c.items())
# print(c.most_common())

# del c['k']

# print(c)
# print(c['k'])

# s = "45 8 9 10 11 14"
# #unpacking
# a,b,*c = map(int,s.split())

# print(a)
# print(b)
#print(c)


# from collections import OrderedDict

# ordered_dict = OrderedDict()

# ordered_dict['b']= 2
# ordered_dict['a']= 1
# ordered_dict['c']= 1

# print(*(key for key in ordered_dict.keys()))
# print(ordered_dict)

# s = "BANANA FRIES 12".split()

# name = ' '.join(s[:-1])
# price = int(s[-1])

# print(name == "BANANA FRIES")
# print(price)

# from collections import Counter

# s = "ddabbbccca"

# x = Counter(s)
# print(x)
# print(x.most_common(3))

# try:
#     1/0
# except ZeroDivisionError as e:
#     print(e)


# ch = 'X'
# N, M = 3,5
# matrix = [[ch  for _ in range(M) ]  for _ in range(N)]

# matrix[0][0] = 'O'

# print(matrix)


# cube = lambda x : x**3
# def fibonacci_generator(n: int): # Note: returns a generator, not a list
#     """
#     Generates Fibonacci numbers up to the nth number (0-indexed).
#     """
#     a, b = 0, 1
#     for i in range(n):
#         yield a
#         #print(f"{i=}, {a=}, {b=}")
#         a, b = b, a + b

# if __name__ == '__main__':
#         n = int(input())
#         fibo_gen = fibonacci_generator(n)
#         for num in fibo_gen:
#                 num = cube(num)
#                 print(num, end=' ')



#return(lis[0:n]) is the same as return list[0:n], same as return (lis[0:n])


# l=(list(range(10)))
# l = list(map(lambda x:x*x, l))
# l = list(filter(lambda x: x>10 and x<30, l))
# print(l)
# s = 'df@gmail.com'

# username, s = map(str,s.split('@'))

# website, ext = map(str,s.split('.'))


# print(username)
# print(website)
# print(ext)

# if '@' not in s or '.' not in s:
#         print("not valid")


# from fractions import Fraction

# fracs=[]
# fracs.append(Fraction(*map(int, "2 5".split())))

# print(fracs)
#  t = reduce(lambda x, y, z: x * y , fracs)
#so everything gets reduced down to the last item which is the same type as the iterable that you started with?
        # answer is no. The reduce() function applies a binary function (a function that takes two arguments) cumulatively to the items of an iterable, from left to right, so as to reduce the iterable to a single output value.
        #No, changing it to t = reduce(lambda x, y, z: x * y * z, fracs) would NOT work as intended and would likely raise a TypeError.
        #if fracs were [1.0, 2.0, 3.0] (floats) and your lambda was lambda x, y: str(x) + str(y) (string concatenation), then t would be a string ("1.02.03.0"), which is a different type than the initial elements in the iterable.




# t = "helloworld"
# res=[]

# res.append("+91"+" "+t[4:7])
# print(res)
# s = ["ok", "ab", "zyy"]


# print(*sorted(s), sep='\n')


# nums = [3,5,7,9]

# def squaring(x):
#     for i, num in enumerate(x):
#         x[i] = num ** 4
#     print(x)
        
# squaring(nums)

# print(id(nums))

# def modify_immutable(x):
#     print(f"Inside function (start) - x: {x}, id(x): {id(x)}")
#     x = x + 10 # This creates a NEW object for x
#     print(f"Inside function (end) - x: {x}, id(x): {id(x)}")

# my_int = 5
# print(f"Outside function (start) - my_int: {my_int}, id(my_int): {id(my_int)}")
# modify_immutable(my_int)
# print(f"Outside function (end) - my_int: {my_int}, id(my_int): {id(my_int)}")


# #outside  5 5
# # inside 5 5
# # 15 15
# #outside  5 5
# #


# def modify_mutable(my_list):
#     print(f"Inside function (start) - my_list: {my_list}, id(my_list): {id(my_list)}")
#     my_list.append(4) # Modifies the original object in place
#     print(f"Inside function (append) - my_list: {my_list}, id(my_list): {id(my_list)}")
#     my_list = [10, 20, 30] # This creates a NEW list object for my_list
#     print(f"Inside function (reassign) - my_list: {my_list}, id(my_list): {id(my_list)}")

# original_list = [1, 2, 3]
# print(f"Outside function (start) - original_list: {original_list}, id(original_list): {id(original_list)}")
# modify_mutable(original_list)
# print(f"Outside function (end) - original_list: {original_list}, id(original_list): {id(original_list)}")

# def memoize(f):
#     cache = {}
#     def wrapper(*args):
#         print(args)
#         if args not in cache:
#             print(f"Calculating {f.__name__}({args}) and caching...") # For demonstration
#             cache[args] = f(*args)
#         return cache[args]
#     return wrapper
# @memoize
# def add(a,b):
#     return a+b

# @memoize
# def fibonacci(n):
#     # This is the original, un-optimized recursive function
#     if n <= 1:
#         return n
#     return fibonacci(n - 1) + fibonacci(n - 2)

# # Usage:
# print("First call to fibonacci(5):")
# print(fibonacci(5))

# print("\nSecond call to fibonacci(5):")
# print(fibonacci(5))

# print("\nCall to fibonacci(4):")
# print(fibonacci(4)) # This will trigger some calculations and some cache hits for F(0) to F(4)

# print("\nCall to add()")
# print(add(4,5))

# def my_func(*args):
#     print(f"type of args: {type(args)}")
#     print(f"val of args: {args}")

# my_func(10)
# my_func("hello")
# my_func([2,3,4],"lol",55)


# a, b, *c = input().split()
# print(type(a))
# print(type(b))
# print(type(c))
# print(a,b)
# print(c)
# print(*c)

# class EvenStream:
#     def __init__(self):
#         self.current = 0
#     def get_next(self):
#         to_return = self.current
#         self.current+=2
#         return to_return
# class  OddStream:
#     def __init__(self):
#         self.current=1
#     def get_next(self):
#         to_return = self.current
#         self.current+=2
#         return to_return

# def print_from_stream(n, stream = EvenStream()):
#     stream.__init__()
#     for _ in range(n):
#         print(stream.get_next())


# print_from_stream(3)
# print_from_stream(4, OddStream())
# print_from_stream(4)

# class BankAccount:
#     def __init__(self, owner: str, initial_balance=0.0):
#         self.owner=owner
#         self.balance = initial_balance
#     def deposit(self, amount):
#         self.balance+=amount
#         print(f"deposit={amount}, {self.balance=}")
#     def withdraw(self, amount):
#         self.balance-=amount
#         print(f"withdraw={amount}, {self.balance=}")
#         if self.balance < 0:
#             print("Insufficient Funds!")

# account1 = BankAccount("Alice")
# account1.deposit(100.50)
# account1.withdraw(50.25)
# account1.withdraw(100.00)

from abc import ABC, abstractmethod
import sys
import os

# 1. Get the directory of the current script (hackerrank_leetcode.py)
current_dir = os.path.dirname(os.path.abspath(__file__))

# 2. Get the parent directory (one level up)
parent_dir = os.path.dirname(current_dir)

# 3. Insert the parent directory path at the beginning of sys.path
#    This tells Python's import mechanism to look there first.
sys.path.insert(0, parent_dir)

from exceptions import *
class BankAccount(ABC):
    def __init__(self, owner: str, initial_balance: float = 0.0):
        # Type hints improve readability and can be used by static analyzers
        self.owner = owner
        # Ensure initial balance isn't negative, though your logic already defaults to 0.0
        if initial_balance < 0:
            raise InvalidAmountError("Initial balance cannot be negative.")
        self.balance = initial_balance

    #@abstractmethod
    def deposit(self, amount: float):
        """Adds funds to the account. Returns True if successful, False otherwise."""
        if amount <= 0:
            raise InvalidAmountError("Error: Deposit amount must be positive.")
        self.balance += amount
        print(f"Deposit successful. New balance: ${self.balance:.2f}.")

    #@abstractmethod
    def withdraw(self, amount: float):
        """Subtracts funds from the account. Returns True if successful, False otherwise."""
        if amount <= 0:
            raise InvalidAmountError("Withdrawal amount must be positive.")
            # print("Error: Withdrawal amount must be positive.")
            # return False
        if amount > self.balance:
            raise InsufficientFundsError("Insufficient fund!")
            # print(f"Insufficient funds! Current balance: ${self.balance:.2f}. Cannot withdraw ${amount:.2f}.")
            # return False
        else:
            self.balance -= amount
            print(f"Withdrawal successful. New balance: ${self.balance:.2f}.")
            return True

    def get_balance(self) -> float:
        """Returns the current account balance."""
        return self.balance

    # def __str__(self) -> str:
    #     """Returns a user-friendly string representation of the BankAccount object."""
    #     return f"* * *Account for {self.owner}, Balance: ${self.balance:.2f}"

    def __repr__(self) -> str:
        """Returns a developer-friendly string representation (useful for debugging lists of objects)."""
        return f"# # #BankAccount(owner='{self.owner}', initial_balance={self.balance})"

    def get_account_type(self) -> str:
        return "Basic Bank Account"


# print("--- 1. Creating a Valid Account ---")
# try:
#     my_account = BankAccount("Alice Wonderland", 500.00)
#     print(f"Account created: {my_account}")
# except BankError as e:
#     print(f"Error creating account: {e}")
# except Exception as e:
#     print(f"An unexpected error occurred during creation: {e}")

# print("\n--- 2. Testing Valid Operations ---")
# if 'my_account' in locals():
#     try:
#         my_account.deposit(150.50)
#         my_account.withdraw(275.25)
#         print(f"Operations successful. Current account: {my_account}")
#     except (InvalidAmountError, InsufficientFundsError) as e:
#         print(f"caught error: {e}")
#     except Exception as e:
#         print(f"Caught an unexpected error during valid operations: {type(e).__name__}: {e}")
#     finally: # This block always executes, regardless of whether an exception occurred
#         print("Valid operations attempt completed.")
# # --- Testing the improved class ---
# print("--- Alice's Account ---")
# account1 = BankAccount("Alice",0.5)
# print(account1) # Uses __str__
# print(repr(account1))
# account1.deposit(100.50)
# account1.withdraw(50.25)
# account1.withdraw(100.00) # This will now correctly show insufficient funds before subtracting



# print("\n--- Bob's Account (with initial balance) ---")

# BankAccount(owner="Bob")
# account2 = BankAccount("Bob", 500.00)
# print(account2)

# account2.withdraw(50.00)
# account2.deposit(200.00)
# account2.withdraw(700.00) # Another insufficient funds example

# print("\n--- Invalid Operations ---")
# account1.deposit(-10.00)
# account1.withdraw(0)

# class SavingsAccount(BankAccount):
#     def __init__(self, owner: str, balance: float, interest_rate: float):
#         super().__init__(owner, balance)
#         self.interest_rate = interest_rate
#     def deposit(self, amount_deposited):
#         super().deposit(amount_deposited)
#         interest_bonus = amount_deposited * (self.interest_rate / 100) # Divide by 100 for percentage
#         self.balance += interest_bonus
#         print(f"deposited {amount_deposited}, with {interest_bonus:.2f}, new balance is {self.balance}")
#     def __str__(self) -> str:
#         return f"Savings Account: {self.owner}, {self.balance=}"
#     def get_account_type(self) -> str:
#         return "Savings Account"
 
    
# class CheckingAccount(BankAccount):
#     def __init__(self, owner:str, balance: float, overdraft_limit: float):
#         super().__init__(owner, balance)
#         self.overdraft_limit = overdraft_limit
#     # def deposit(self, amount):
#     #     print("checking acc deposit():")
#     def withdraw(self, amount_withdraw):
#         if amount_withdraw <= self.balance:
#             self.balance-=amount_withdraw
#             print(f"Withdrawing ${amount_withdraw}, new balance is ${self.balance}")
#         elif amount_withdraw <= self.overdraft_limit + self.balance:
#             self.balance-=amount_withdraw #negative
#             print(f"Overdrafted. Withdrawing ${amount_withdraw}, new balance is ${self.balance}")
#         else:
#             print(f"Exceeded overdraft limit. Cannot withdraw.")
#     def __str__(self) -> str:
#         return f"Checking Account: {self.owner}, {self.balance=}"
#     def get_account_type(self) -> str:
#         return "Checking Account"

# from typing import List

# def process_monthly_statements(accounts: List[BankAccount]):
#     for acc in accounts:
#         if isinstance(acc, SavingsAccount):
#             monthly_interest_rate = acc.interest_rate / 100 / 12 # Convert percentage to decimal and then to monthly
#             acc.balance *= (1 + monthly_interest_rate)
#             print(f"` ` `saving acc balance is {acc.balance: .2f}")
#         print(acc.get_account_type())
#         print(acc.get_balance())
        # apply monthly interest
    



# save = SavingsAccount("RZ",100, 2)
# save.deposit(50)
# print(save)
# check = CheckingAccount("REN",90, 30)
# check.withdraw(10)

# check.withdraw(85)
# check.withdraw(80)
# check.deposit(1)
# print(check)

# basic = BankAccount("Alice", 100)
# print(basic)

# accounts = [save, check, basic]
# process_monthly_statements(accounts)


# #moved it to Mixin.py
# from Mixin import AuditableMixin
# # --- Core BankAccount classes ---
# from abc import ABC, abstractmethod

# class BankAccount(ABC):
#     def __init__(self, account_number, initial_balance):
#         print(f"Init() in {self.__class__.__name__} in BankAccount")
#         self._account_number = account_number
#         self._balance = initial_balance

#     @abstractmethod
#     def deposit(self, amount):
#         pass

#     @abstractmethod
#     def withdraw(self, amount):
#         pass

#     def get_balance(self):
#         return self._balance

# # --- Combining Mixin with a Concrete Account ---
# class CheckingAccount(AuditableMixin, BankAccount): # Inheriting from Mixin FIRST is common
#     def __init__(self, account_number, initial_balance, overdraft_limit):
#         print(f"Init() in {self.__class__.__name__} in CheckingAcc")
#         # Call super() to properly initialize AuditableMixin and BankAccount
#         super().__init__(account_number, initial_balance)
#         self._overdraft_limit = overdraft_limit
#         self._record_audit_event(f"Checking account created with limit {overdraft_limit}")

#     def deposit(self, amount):
#         if amount > 0:
#             self._balance += amount
#             self._record_audit_event(f"Deposited {amount}")
#             print(f"Checking Deposited {amount}. New balance: {self._balance}")
#         else:
#             print("Deposit amount must be positive.")
#             self._record_audit_event(f"Failed deposit of {amount}")

#     def withdraw(self, amount):
#         if amount <= 0:
#             print("Withdrawal amount must be positive.")
#             return

#         if self._balance - amount >= -self._overdraft_limit:
#             self._balance -= amount
#             if self._balance < 0:
#                 print(f"Checking Overdraft! New balance: {self._balance}")
#                 self._record_audit_event(f"Overdraft withdrawal of {amount}")
#             else:
#                 print(f"Checking Withdrew {amount}. New balance: {self._balance}")
#                 self._record_audit_event(f"Withdrew {amount}")
#         else:
#             print("Withdrawal exceeds overdraft limit.")
#             self._record_audit_event(f"Failed withdrawal of {amount} (exceeds limit)")


# --- Usage ---
# my_checking = CheckingAccount("C987", 500, 200)
# my_checking.deposit(100)
# my_checking.withdraw(750) # This will cause an overdraft
# my_checking.withdraw(100) # This will exceed the limit

# print("\n--- Checking Account Audit Trail ---")
# for entry in my_checking.get_audit_trail():
#     print(entry)

# # Method Resolution Order (MRO)
# print(CheckingAccount.mro())

# a,b,*c = "3 4 5 6 7 8".split()

# print(type(c))

# c=['5','6','7']
# print(*c)
# for x in c:
#     print(x, end=' ')
# print()


# def introduce_person(name, age, **details):
#     print(f"Name: {name}")
#     print(f"Age: {age}")
#     print(type(details))
#     if details:
#         print("Additional details:")
#         for key, value in details.items():
#             print(f"  {key.replace('_', ' ').title()}: {value}")

# introduce_person("Alice", 30, city="New York", occupation="Engineer", 
#                  hobbies=["reading", "hiking"])

# def log_message(level, message, *extra_info, **metadata):
#     print(f"[{level}] {message}")
#     if extra_info:
#         print(f"  Extra Positional: {extra_info}")
#     if metadata:
#         print(f"  Metadata: {metadata}")

# log_message("INFO", "User logged in", "from_ip_127.0.0.1","from_ip_127.0.0.2", user_id=123, session_id="abc")
# # Output:
# # [INFO] User logged in
# #   Extra Positional: ('from_ip_127.0.0.1',)
# #   Metadata: {'user_id': 123, 'session_id': 'abc'}


#Q1
# def display_items(*items):
#     print(type(items))
#     for item in items:
#         print(item)


# # display_items("apple", "banana", "cherry")
# # display_items("apple", False, 2.2)
# # display_items()

# #Q2
# def create_profile(name: str, age: int, **details):
#     print(f"{name=}")
#     print(f"{age=}")
#     for detail in details.items():
#         print(detail)

# # create_profile(name="Alice", age=30,  city="New York", occupation="Engineer")
# # create_profile(age=30, name="Alice",  city="New York", occupation="Engineer")
# # create_profile(name="Bob", age=25)
# # create_profile(name="Charlie", age=35, hobbies=["reading", "gaming"], admin=True)


# #Q3
# def process_order(order_id, *item_names, **options):
#     print(f"Q3 {order_id}")
#     for name in item_names:
#         print(f"{name}")
#     for k,v in options.items():
#         print(f"{k=},{v=}")

# # process_order(101, "laptop", "mouse", delivery_date="2025", express_shipping=True)
# # process_order(202, "Keyboard", customer_notes="Gift wrapping requested")
# # process_order(303,priority="High")
# # process_order(303,"High")

# #Q4
# def calculate_sum(a,b,c):
#     return a+b+c

# nums = [10,20,30]

# print(calculate_sum(*nums))

# #Q5
# def show_product_info(name, price, category):
#     print(f"{name=}")
#     print(f"{price=}")
#     print(f"{category=}")

# product_details = {
#     "name": "laptop",
#     "price": 1200,
#     "category": "Electronics"
# }

# show_product_info(**product_details)


# #Q6
# def config_server(host, port, username="admin", **extra_settings):
#     print(f"{host=}")
#     print(f"{port=}")
#     print(f"{username=}")
#     for setting in extra_settings.items():
#         print(setting)

# server_address = ["localhost", 8080]
# settings = {"timeout": 30, "log_level": "DEBUG"}

# config_server(*server_address, username="sysadmin", **settings)

# #Q7

# def update_user_profile(user_id, **updates):
#     print(f"{user_id}")
#     for update in updates.items():
#         print(update)


# initial_updates = {"email": "new@example.com", "phone": "123-456-7890"}

# update_user_profile(user_id=101, **initial_updates, status="active")
# update_user_profile(user_id=202, status="inactive", city="NY")

# #Q8

# list1=[1,2,3]
# list2=[7,8,9]

# combined_list = [*list1, 4, *list2, 10]
# print(combined_list)

# #Q9
# dict1={'a':1, 'b':2}
# dict2={'c':3, 'd':4}
# merged_dict = {**dict1, **dict2}
# print(merged_dict)


# #Q10 #note the order matters here
# base_config = {'host': 'localhost', 'port': 8080}
# user_config = {'port': 9000, 'timeout': 60}
# last_config = {'port':9999, 'timeout':61}

# final_config = {**base_config,  **user_config, **last_config}

# print(final_config)


# Error Handling
#  File I/O to make your data persistent, 
# followed by Context Managers and Logging, and 
# finally, Unit Testing to ensure everything works correctly.

# s = "hello"

# t = "okay"

# for a,b in zip(s,t):
#     print(ord(a),ord(b))

# print(ord('z'),ord('a'))

# def isValidCode(s):
#     if not s:
#         return False
#     return all(c.isalnum() or c == "-" for c in s)

# print(isValidCode(""))

# import heapq

# nums = [5,1,8,3,2]

# heapq.heapify(nums)

# print(nums)

# heapq.heappush(nums,0)
# print(nums)

# s = "abc"
# print("hi")
# for i in range(len(s)- 1,-1, -1):
#     print(i)


s="hello"
t = "monkey"
x = "powersssz"

# for a,b,c in zip(s,t,x):
#     #print(f"{a} {b} {c}")
#     print(a,b,c, sep=" ")

#from math import inf
s = ["he",t,x]
s=[]
# for i in range(len(s)-1):
#     print(s[i], s[i+1])
# for a,b in zip(s,s[1:]):
#     print(a,b)
# min_len = inf

# for a in s:
#     min_len=min(min_len, len(a))
# min_len = min(len(a) for a in s)
# print(f"{min_len=}")

# min_len


s = "hellocarworld"

# for i in range(len(s)):
#     print(s[i:i+3])

# for i in range(len(s)-1,-1,-1):
#     print(s[i])
# from typing import List
# def foo(x : List[int]):
#     y = x
#     y[0] = 'x'
#     return

# a = [1,2,3,4]

# foo(a[:])
# print(a)

# import collections
# def abc(root, targetSum):
#     if not root:
#         return False
#     stack = collections.deque([(root, root.val)])
#     while stack:
#         root, total = stack.pop()
#         if root.right:
#             stack.append((root.right,total+root.val))
#         if root.left:
#             stack.append((root.left, total+root.val))
#         if not root.left and not root.right and total == targetSum:
#             return True
#     return False
# abc(None, 10)


# x = "a T x g,"

# y = [ch.lower() for ch in x if ch.isalpha()]

# print(y)

# class Solution:
#     def isPalindrome(self, s: str) -> bool:
#         # new_s = [ch.lower() for ch in s if ch.isalnum()]
#         # new_s = ''.join(new_s)
#         # return new_s == new_s[::-1]
#         i, j=0, len(s)-1
#         while i < j:
#             while not s[i].isalnum():
#                 i+=1
#             while not s[j].isalnum():
#                 j-=1
#             if i<len(s) and j<len(s) and s[i].isalnum() and s[j].isalnum() and s[i].lower() != s[j].lower():
#                 return False
#             i+=1
#             j-=1
#         return True

# obj = Solution()
# s = "A man, a plan, a canal: Panama"
# obj.isPalindrome(s)


# from collections import Counter
# nums =[5,6,6,6,6,4,4,4,55,5]

# counter = Counter(nums)
# x = counter.most_common()

# y = max(counter.items(), key=lambda item: item[1])

# print(x)
# print(y)

# s = 5
# s.bit_count

# from collections import defaultdict
# x = defaultdict(list)

# eq = [[3,5],[6,8]]
# values=["3.0","4.0"]
# for a, b,val in zip(eq,values):
#     print(a,b,val)


# x = [3,4,5]

# def getA(x):
#     x.append('s')

#     def addy():
#         x.append('y')
#     addy()

# print(x)
# getA(x)
# print(x)

# my_dict = defaultdict(list)

# my_dict['6'].append(5)

# print(my_dict)

# if not my_dict['6']:
#     print("hi")


# class Car:
#     # Class variable: shared by all instances of Car
#     # This keeps track of how many cars have been created
#     total_cars_created = 0
#     # Class variable: a list of valid colors for any car
#     VALID_COLORS = ["red", "blue", "green", "black", "white"]

#     def __init__(self, make, model, color):
#         self.make = make      # Instance variable
#         self.model = model    # Instance variable
#         self.color = color    # Instance variable
#         Car.total_cars_created += 1 # Increment class variable when a new car is made
#         print(f"A new {self.color} {self.make} {self.model} was created!")

#     # --- 1. Instance Method (requires 'self') ---
#     # Useful for: Actions related to a specific car object.
#     def drive(self):
#         """Makes a specific car drive."""
#         print(f"The {self.color} {self.make} {self.model} is driving.")

#     # --- 2. Class Method (requires 'cls') ---
#     # Useful for:
#     #   a) Operating on class-level data (e.g., total_cars_created, VALID_COLORS).
#     #   b) Creating alternative constructors (different ways to build a Car).
#     @classmethod
#     def get_total_cars(cls):
#         """Returns the total number of Car instances created."""
#         print("* * * ", cls.VALID_COLORS)
#         return cls.total_cars_created # Accessing the class variable using cls

#     @classmethod
#     def create_red_car(cls, make, model):
#         """An alternative constructor to easily create a red car."""
#         if "red" not in cls.VALID_COLORS:
#             raise ValueError("Red is not a valid color for this car model!")
#         return cls(make, model, "red") # Use 'cls' to call the __init__ method

#     # --- 3. Static Method (requires no 'self' or 'cls') ---
#     # Useful for:
#     #   a) Utility functions that logically belong to the class but don't
#     #      need access to instance data or class data.
#     #   b) They're just functions living inside the class's namespace.
#     @staticmethod
#     def honk_horn(sound="Beep beep!"):
#         """A generic horn sound, doesn't need a specific car to honk."""
#         print(sound)

#     @staticmethod
#     def is_valid_color(color_name):
#         """Checks if a given color is a valid car color (utility function)."""
#         # This could also be a class method if it needed to access cls.VALID_COLORS
#         # directly, but here we're passing the color_name, so it's fine as static.
#         return color_name.lower() in [c.lower() for c in Car.VALID_COLORS]


# # --- Demonstrating Usage ---

# print("--- Creating Cars ---")
# car1 = Car("Toyota", "Camry", "blue")
# car2 = Car.create_red_car("Honda", "Civic") # Using the class method as an alternative constructor
# car3 = Car("Ford", "F-150", "green")

# print("\n--- Using Instance Methods ---")
# car1.drive()
# car2.drive()

# print("\n--- Using Class Methods ---")
# # Accessing class-level data
# print(f"Total cars created: {Car.get_total_cars()}")
# # You can also call class methods on an instance, but it's less common for this type
# print(f"Total cars created (via car1): {car1.get_total_cars()}")

# print("\n--- Using Static Methods ---")
# # Calling static method directly from the class (most common)
# Car.honk_horn()
# Car.honk_horn("Vroom!")

# # Calling static method from an instance (works, but usually less intuitive)
# car1.honk_horn()

# # Utility function
# print(f"Is 'yellow' a valid car color? {Car.is_valid_color('yellow')}")
# print(f"Is 'white' a valid car color? {Car.is_valid_color('white')}")



#PRIME
# import math
# def generate_primes(n):
#     is_prime = [True] * n
#     is_prime[0] = False # 0 is not prime
#     is_prime[1] = False # 1 is not prime

#     # Iterate from 2 up to the square root of n.
#     # We only need to check factors up to sqrt(n) because if a number x has a factor > sqrt(n),
#     # it must also have a factor < sqrt(n).
#     for p in range(2, int(math.sqrt(n)) + 1):
#         # If is_prime[p] is still True, then p is a prime number.
#         if is_prime[p]:
#             # Mark all multiples of p as not prime.
#             # Start marking from p*p, as smaller multiples (p*2, p*3, ...)
#             # would have already been marked by smaller primes.
#             for multiple in range(p * p, n, p):
#                 is_prime[multiple] = False
#     return is_prime

#print(generate_primes(5))

# from collections import deque

# q = deque()


# from typing import List
# def countIslands( grid: List[List[int]], k: int) -> int:
#         res=0
#         m = len(grid)
#         n = len(grid[0])
#         dr=[-1,1,0,0]
#         dc=[0,0,-1,1]
#         for i in range(m):
#             for j in range(n):
#                 if grid[i][j] > 0:
#                     #total_value = self.dfs(grid,i,j)

#                     q = deque([(i,j)]) #when u pop later (not in, to avoid confusion), add it to total_val
#                     total_value = grid[i][j]
#                     grid[i][j] = 0 #mark as visited
#                     #thought need sum, but just need to run BFS completely at each possible cell and empty it by the end, so we can just keep a local var
#                     while q:
#                         r, c = q.popleft()

#                         for x in range(4):
#                             nr = dr[x] + r
#                             nc = dc[x] + c
#                             if 0<=nr<m and 0<=nc<n and grid[nr][nc] > 0:
#                                 q.append((nr,nc))
#                                 total_value+=grid[nr][nc]
#                                 grid[nr][nc]=0 #mark as vis

#                     if total_value % k == 0:
#                         res+=1
#         return res


# grid =[[0,0,0],[0,0,1],[11,0,6],[0,10,2],[0,0,0],[8,0,0]]
# k =19



# countIslands(grid, k)

# import itertools, math

# x = [4,5,6]
# print(math.prod(x))

# from typing import List
# class Solution:
#     def _getCount(self, index):
#         target_index = []
#         k=0
#         for _ in range(len(index)):
#             target_index.append(k)
#             k+=2
#         res=0
#         for i, target_i in zip(index, target_index):
#             res+=abs(i - target_i)
#         return res

#     def minSwaps(self, nums: List[int]) -> int:
#         count=0
#         even, odd=[], []
#         for i,num in enumerate(nums):
#             if num % 2 == 0:
#                 even.append(i)
#             else:
#                 odd.append(i)
#         if abs(len(even) - len(odd)) > 1:
#             return -1
#         if len(even) > len(odd):
#             count = self._getCount(even)
#         else:
#             count = self._getCount(odd)
#         return count




# nums = [2,4,5,7]

# q = Solution()
# print(q.minSwaps(nums))


# x = [1,2,1,3,2]
# y = [5,3,4,6,2]
# from typing import List
# import heapq
# class Solution:
#     def maxSumDistinctTriplet(self, x: List[int], y: List[int]) -> int:
#         mp = defaultdict(int) # x val to y val directly, not index
#         n = len(x)
#         for i, x_ele in enumerate(x):
#             mp[x_ele] = max(mp[x_ele], y[i])
#         if len(mp) < 3:
#             return -1
#         heap=[]
#         for val in mp.values():
#             heapq.heappush(heap, val)
#             if len(heap) > 3:
#                 heapq.heappop()
#         total=0
#         while heap:
#             total+=heapq.heappop(heap)
#         # heapq.heappush(heap,5)
#         # heapq.heappush(heap,15)
#         # heapq.heappush(heap,25)
#         # heapq.heappush(heap,1)
#         # print(heapq.heappop(heap))
#         # print(heap)
#         # largest_y_values = sorted(mp.values(), reverse=True)[:3]
#         # return sum(largest_y_values)
#         return total


# q = Solution()
# q.maxSumDistinctTriplet(x,y)

# largest_keys = sorted(mp.keys(), key=lambda k: y[mp[k]], reverse=True)[:3]
# print(largest_keys)  # keys corresponding to largest y[mp[key]]
# largest_y_values = [y[mp[k]] for k in largest_keys]
# print(largest_y_values)  # the actual y values
    
# mp = defaultdict(int)

# x = [4,6,3,7,9]

# for num in x:
#     mp[num]+=1

# y= sorted(mp.items())
# print(sorted(mp))
# print(y)

# s = "12345"
# map(int, s)

# print(ord('a'))
# print(ord('A'))

# from collections import deque
# q = deque()

# q.append((1,2))
# q.append((4,2))
# q.append((2,2))

# print(q)


# class Solution:
    # MAX_SUBSTRING_VAL = 99  # Adjust based on actual constraints if known
    # is_prime = [True] * (MAX_SUBSTRING_VAL + 1)
    
#     # Initialize the sieve only once when the class is loaded.
#     # This ensures it's not re-computed for every test case.
#     if is_prime[0]: # Check if it's the first time initialization (a simple flag)
#         is_prime[0] = is_prime[1] = False
#         for p in range(2, int(MAX_SUBSTRING_VAL**0.5) + 1):
#             if is_prime[p]:
#                 for multiple in range(p * p, MAX_SUBSTRING_VAL + 1, p):
#                     is_prime[multiple] = False
#     def sumOfLargestPrimes(self, s: str) -> int:
#         found_primes=set()
#         for i in range(len(s)):
#             for j in range(i+1,len(s)+1):
#                 n = int(s[i:j])
#                 if 0<=n<=self.MAX_SUBSTRING_VAL and self.is_prime[n]:
#                     found_primes.add(n)
#         if len(found_primes) > 3:
#             sorted_primes=sorted(found_primes, reverse=True)
#             return sum(sorted_primes[:3])
#         else:
#             return sum(found_primes) 
# a = Solution()
# s = "111"
# #print(a.sumOfLargestPrimes(s))
# def is_prime(n):
#     for i in range(2, int(n**0.5) + 1):
#         print(i)
#         if n % i == 0:
#             return False
#     return True

# print(is_prime(169))
# from typing import List
# class Solution:
#     def canPartitionGrid(self, grid: List[List[int]]) -> bool:
#         row_sums, col_sums=[],[]
#         for row in grid:
#             prev_sum = 0
#             if row_sums:
#                 prev_sum = row_sums[-1]
#             row_sums.append(sum(row) + prev_sum)
#         for a,b in zip(row_sums,row_sums[1:]):
#             if a*2 == b:
#                 return True
#         for col in zip(*grid):
#             prev_sum = 0
#             if col_sums:
#                 prev_sum = col_sums[-1]
#             col_sums.append(prev_sum + sum(col))
#         for a,b in zip(col_sums, col_sums[1:]):
#             if a*2==b:
#                 return True
#         return False 

# s = Solution()

# grid =[[100000,100000,92687],[11,22,33]]
# #print(s.canPartitionGrid(grid))


# x = sum([sum(r) for r in grid])
# print(x)

# from sortedcontainers import SortedList

# sl = SortedList()

# sl.add(5)
# sl.add(2)
# sl.add(15)
# sl.add(1)

# print(sl)
# import bisect

# my_list = [1, 2, 4, 4, 6, 8, 10]
# value = 11
# def rz_bisect_left(nums, value):
#     lo, hi = 0, len(nums)
#     while lo < hi:
#         mid = (hi + lo) // 2
#         if nums[mid] < value:
#             lo = mid + 1
#         else:
#             hi = mid
#     return lo

# def rz_bisect_right(nums, value):
#     lo, hi = 0, len(nums)
#     while lo < hi:
#         mid = (hi + lo) // 2
#         if nums[mid] > value:
#             hi = mid
#         else:
#             lo = mid + 1
#     return lo
# # bisect_left finds the index to insert '4' before the existing '4's
# left_index = bisect.bisect_left(my_list, value)
# print(f"bisect_left for {value}: {left_index=}")
# left_index = rz_bisect_left(my_list, value)
# print(f"rz_left for {value}: {left_index=}")

# Expected output: bisect_left for 4: 2
# The index is 2, because we can insert 4 before the first 4.
# my_list[2] is 4.

# # bisect_right finds the index to insert '4' after the existing '4's
# right_index = bisect.bisect_right(my_list, value)
# print(f"bisect_right for {value}: {right_index=}")
# right_index = rz_bisect_right(my_list, value)
# print(f"rz_right for {value}: {right_index=}")

# # Expected output: bisect_right for 4: 5
# # The index is 5, because we can insert 4 after the last 4.
# # my_list[5] is 6.

# #TODO: write our own bisect_left & bisect_right functions


# my_list = [1, 3, 5, 7, 9]
# value_to_find = 5

# i = bisect.bisect_left(my_list, value_to_find)
# if i < len(my_list) and my_list[i] == value_to_find:
#     print(f"Found {value_to_find} at index {i}")
# else:
#     print(f"{value_to_find} not found in list")

# # Example for a value not in the list
# value_to_find = 6
# j = bisect.bisect_left(my_list, value_to_find)
# if j < len(my_list) and my_list[j] == value_to_find:
#     print(f"Found {value_to_find} at index {j}")
# else:
#     print(f"{value_to_find} not found in list")

# from typing import List

# class Solution:
#     def dfs(self, board: List[List[str]], word: str, r: int, c: int, index: int) -> bool:
#         m, n, wlen = len(board), len(board[0]), len(word)
#         if r < 0 or r >= m or c < 0 or c >= n or index >= wlen or board[r][c] == '-' or \
#         word[index] != board[r][c]:
#             return False
#         temp_char = board[r][c]
#         board[r][c] = '-' #temporarily mark it
#         C = self.dfs(board, word, r, c+1, index+1)
#         A = self.dfs(board, word, r+1, c, index+1)
#         B = self.dfs(board, word, r-1, c, index+1)
#         D = self.dfs(board, word, r, c-1, index+1)
#         board[r][c] = temp_char #restore it
#         return A + B + C + D > 0
#     def exist(self, board: List[List[str]], word: str) -> bool:
#         m, n = len(board), len(board[0])
#         for i in range(m):
#             for j in range(n):
#                 if board[i][j] == word[0]:
#                     #start search
#                     if self.dfs(board, word, i, j, 0):
#                         return True
#         return False
# board = [["A","B","C","D"],["S","F","C","S"],["A","D","E","E"]]

# class Solution:
#     def countPrimes(self, n: int) -> int:
#         if n<=1:
#             return 0
#         isPrime = [ True ] * n
#         isPrime[0] = False
#         isPrime[1] = False
#         for i in range(2,int(n**0.5)+1):
#             if isPrime[i]:
#                 for multiple in range(i*i, n, i):
#                     isPrime[multiple] = False
#         print(isPrime)
#         return sum(isPrime)
            
# q = Solution()
# q.countPrimes(10)


# a = "tzzzreea"

# from collections import Counter
# freq = Counter(a)
# print(freq)
# b = sorted(freq.items(), key = lambda x : (-x[1], x[0]))
# print(b)

# c = sorted(freq.items(), key = lambda x : (x[1], x[0]))
# print(c)

# print(*(val for val, f in freq.items()))

# v = [[5,'a'],[4,'e'],[6,'c']]
# print(v)
# v.sort(key = lambda x : x[1])

# print(v)

# import pandas as pd

# # Create a dictionary of data
# data = {
#     'Name': ['Alice', 'Bob', 'Charlie', 'David'],
#     'Age': [25, 30, 35, 40],
#     'City': ['New York', 'Los Angeles', 'Chicago', 'Houston']
# }

# # Create a DataFrame
# df = pd.DataFrame(data)

# df['Salary'] = [70000, 80000, 90000, 100000]

# # Print the DataFrame
# y = df['Salary'] > 80000
# print(y)
# x_df = df[(df['Salary'] > 80000) & (df['Age']>35 ) ]
# print(x)

# # Correctly filter the DataFrame for salaries greater than 80000
# high_salary_df = df[(df['Salary'] > 80000) | (df['Age'] >= 30)]

# # Print the filtered DataFrame
# print(high_salary_df)


# from typing import List
# class Solution:
#     def canSplit(self, nums: List[int], k: int, target: int) -> bool:
#         cnt, total = 0, 0 #cnt is split count
#         for num in nums:
#             total+=num
#             #if it's total == target, just keep adding, might have 0s later,
#             #but i dont think that makes a difference.. since we don't care how its split
#             # 0 can be at the front of the next split also 
#             if total > target:
#                 #go back, instead of substract, just set new
#                 total = num # new total now
#                 cnt+=1
#         # reached the end. Do we need to check total <= target? think not since it's checked up there at each +=num.
#         return cnt == k - 1


#     def splitArray(self, nums: List[int], k: int) -> int:
#         lo, hi = max(nums), sum(nums) #note the search space/boundaries.
#         while lo < hi:
#             # mid is the target answer we're guessing
#             mid = (hi + lo) // 2
#             if self.canSplit(nums, k, mid) == False:
#                 lo = mid + 1
#             else:
#                 hi = mid
#         return lo

# q = Solution()
# nums = [1,4,4]
# k=3
# q.splitArray(nums,k)
# x = [[False for _ in range(5)] for _ in range(2)]
# print(x)
# grid = [[False for _ in range(3)] for _ in range(6)]
# sub_grid = [[[False for _ in range(9)] for _ in range(3)] for _ in range(3)] #

# #print(sub_grid)
# from typing import List
# class Solution:
#     def isValidSudoku(self, grid: List[List[str]]) -> bool:
#         rows = [ [False for _ in range(9)] for _ in range(9) ] #row[0][5] does row 0 contain 5?
#         cols =  [ [False for _ in range(9)] for _ in range(9) ]
#         sub_grid = [[False for _ in range(3)] for _ in range(3)] #
#         for r in range(len(grid)):
#             for c in range(len(grid[0])):
#                 if grid[r][c] == '.':
#                     continue
#                 num = int(grid[r][c]) - 1 
#                 if rows[r][num] or cols[c][num]:
#                     return False
#                 if sub_grid[r//3][c//3]:
#                     return False
#                 rows[r][num] = cols[c][num] = sub_grid[r//3][c//3] = True
#         return True


# # board = [["5","3",".",".","7",".",".",".","."],["6",".",".","1","9","5",".",".","."],[".","9","8",".",".",".",".","6","."],["8",".",".",".","6",".",".",".","3"],["4",".",".","8",".","3",".",".","1"],["7",".",".",".","2",".",".",".","6"],[".","6",".",".",".",".","2","8","."],[".",".",".","4","1","9",".",".","5"],[".",".",".",".","8",".",".","7","9"]]
# # q = Solution()
# # print(q.isValidSudoku(board))

# grid = [[ x for x in range(3)] for _ in range(5)]
# print(grid) #5 rows, 3 cols
# print(grid[:])
# for row in grid:
#     print(row)

# for col in zip(*grid):
#     print(col)

# A = [1, 2, 3]

# # Using A[:]
# B = A[:]  # B is a new list, a shallow copy of A
# print(A)  # Output: [1, 2, 3]
# print(B)  # Output: [1, 2, 3]

# # Now, modify A
# A.append(4)
# print(A)  # Output: [1, 2, 3, 4]
# print(B)  # Output: [1, 2, 3] - B remains unchanged


# original_list = [1, 2, [3, 4]]

# import copy
# # Create a shallow copy using A[:]
# shallow_copy = original_list[:]
# deep_copy = copy.deepcopy(original_list)
# print(f"Original list before modification: {original_list}")
# print(f"Shallow copy before modification: {shallow_copy}")
# print(f"Deep copy before modification: {deep_copy}")

# # Now, let's modify a mutable object (the nested list) inside the shallow copy
# shallow_copy[2].append(5)

# print("\n--- After modifying the nested list in the shallow copy ---")
# print(f"Original list after modification: {original_list}")
# print(f"Shallow copy after modification: {shallow_copy}")
# print(f"Deep copy after modification: {deep_copy}")


# x = [3,5,7,8,9,11]

# print(x[::-1])

# print([a for a in reversed(x)])

# matrix = [[1,2,3],[4,5,6],[7,8,9]]
# # matrix[:] = reversed(matrix)
# print(matrix)
# for a in reversed(matrix):
#     print(a)
# from typing import List
# class Solution:
#     def rotate(self, matrix: List[List[int]]) -> None:
#         """
#         Do not return anything, modify matrix in-place instead.
#         """
#         matrix[:] = reversed(matrix)
#         n = len(matrix)
#         for i in range(n):
#             for j in range(i+1,n,1):
#                 print(i,j)
#                 #swap
#                 matrix[i][j], matrix[j][i] = matrix[j][i], matrix[i][j]
# q = Solution()
# q.rotate(matrix)
# print(matrix)


# from typing import List
# class Solution:
#     def subarraySum(self, nums: List[int], k: int) -> int:
#         m = defaultdict(int)
#         m[0] = 1
#         n = len(nums)
#         count, total = 0, 0
#         prefix_sum = [nums[0]] * n
#         # for i, num in enumerate(nums):
#         #     prefix_sum[]
#         for i in range(1,n):
#             prefix_sum[i] = prefix_sum[i-1] + nums[i]
#         print(prefix_sum)
#         for i, num in enumerate(nums):
#             total+=num
#             prev_sum = prefix_sum[i] - total
#             if prev_sum in m:   
#                 count+=m[prev_sum]
#             m[prefix_sum[i]]+=1
#         return count
    
# nums = [1,1,1,1]
# q = Solution()
# q.subarraySum(nums,3)

# class Solution:
#     def findDuplicates(self, nums: List[int]) -> List[int]:
#         ans=[]
#         for num in nums:
#             index = num - 1 #use num as index
#             if nums[index] < 0: #negative, so seen before
#                 ans.append(num) #flip neg to pos
#             else:
#                 #mark as seen
#                 nums[index] = -nums[index]
#         return ans


# s = set([x for x in range(1,3)])
# print(s)

# print(s)
# print(s.pop())

# from collections import OrderedDict

# od = OrderedDict()

# od['xapple'] = 3
# od['banana'] = 2
# od['orange'] = 5

# print(od)

# for key, val in od.items():
#     print(key,val)



# t = tuple(sorted(s))
# print(t)

# s = "is2 sentence4 This1 a3"

# s = s.split()
# print(s)
# s.sort(key=lambda x: x[-1])
# print(s)

# s= "345"

# print(s.isnumeric())

# from collections import Counter
# s = [1,2,3,3,2,1,5,5,5,5,5,5]
# counts = Counter(s)

# print(counts.items())
# print(counts)

# my_counter = Counter({'apple': 3, 'banana': 2, 'orange': 1})
# print(f"Before deletion: {my_counter}")

# # del my_counter['banana']
# my_counter.pop('banana')
# print(f"After deleting 'banana': {my_counter}")

# s = "  TO BE OR NOT TO BE    "
# print(s.lstrip())
# s = s.split()
# print(s)
# n = len(s)
# max_word=0
# max_word = max(len(x) for x in s)
# max_word = max(map(len,s))
# # print(*(len(x) for x in s))
# ans=[['' for _ in range(n)] for _ in range(max_word) ]

# print(ans)
# from typing import List
# import heapq
# class Solution:
#     def lastStoneWeight(self, stones: List[int]) -> int:
#         max_heap = stones[:]
#         heapq.heapify(max_heap)
#         stones.append(99)
#         print(max_heap)
#         print(stones)
#         return 0
#         # while len(max_heap) > 1:
#         #     x = heapq.heappop(max_heap)
#         #     y = heapq.heappop(max_heap)
#         #     heapq.heappush(max_heap, x-y)
#         # return max_heap[0]

# q = Solution()
# stones=[6,1,2,3,4]
# q.lastStoneWeight(stones)


# s = "TO BE OR NOT123 TO BE"

# s = s.split()
# x = max(len(word) for word in s)
# y = list(map(len, s))
# print(x)
# print(y)

# from typing import List
# from math import inf
# class Solution:
#     def sortedSquares(self, nums: List[int]) -> List[int]:
#         mid = 0
#         smallest=inf
#         for i, num in enumerate(nums):
#             if abs(num) < smallest:
#                 #discard negative sign
#                 smallest = abs(num) # new smaller
#                 mid = i
#         ans = [nums[mid]**2]
#         left = mid - 1
#         right = mid + 1
#         n = len(nums)
#         while left >= 0 or right < n:
#             if left >= 0 or abs(nums[left]) < abs(nums[right]):
#                 ans.append(nums[left]**2)
#                 left-=1
#             else:
#                 ans.append(nums[right]**2)
#                 right+=1
#         return ans

# from typing import List
# class Solution:
#     def trap(self, height: List[int]) -> int:
#         stk = [] # (val, index)
#         res = 0
#         for i, ht in enumerate(height):
#             while stk and stk[-1][0] < ht:
#                 h_1, i_1 = stk.pop()
#                 if stk:
#                     h_2, i_2 = stk[-1]
#                     width = i - i_2 - 1
#                     h = min(ht, h_2) - h_1
#                     res+= width * h
#             stk.append((ht, i))
#         return res
# qq = Solution()
# height = [0,1,0,2,1,0,1,3,2,1,2,1]
# qq.trap(height)

# from collections import deque
# import collections
# from typing import List
# class Solution:
#     def maxSlidingWindow(self, nums: List[int], k: int) -> List[int]:
#         dq = collections.deque()
#         ans = []
#         left = 0
#         for i in range(len(nums)):
#             while dq and dq[-1] < nums[i]:
#                 dq.pop()
#             dq.append(nums[i])
#             if i - left == k - 1:
#                 ans.append(dq[0])
#                 if dq[0] == nums[left]:
#                     dq.popleft()
#                 left+=1
#         return ans
# nums = [4, 1, 5, 2, 5, 3]
# q = Solution()
# q.maxSlidingWindow(nums,3)

# import heapq
# min_heap= []
# heapq.heappush(min_heap, (5,2))
# heapq.heappush(min_heap, (6,1))
# heapq.heappush(min_heap, (4,0))

# print(heapq.heappop(min_heap))
# print(heapq.heappop(min_heap))
# print(min_heap[0])\\\


# def maximumGap(nums):
#     n = len(nums)
#     if n < 2:
#         return 0

#     min_val, max_val = min(nums), max(nums)
#     if min_val == max_val:
#         return 0

#     # Step 1: calculate bucket size
#     bucket_size = math.ceil((max_val - min_val) / (n - 1))
#     bucket_count = (max_val - min_val) // bucket_size + 1

#     # Step 2: create buckets
#     buckets = [[math.inf, -math.inf] for _ in range(bucket_count)]
#     # Each bucket stores [min_in_bucket, max_in_bucket]

#     # Step 3: place numbers into buckets
#     for num in nums:
#         if num == min_val or num == max_val:
#             continue  # skip extremes, handle separately
#         idx = (num - min_val) // bucket_size
#         buckets[idx][0] = min(buckets[idx][0], num)
#         buckets[idx][1] = max(buckets[idx][1], num)

#     # Step 4: scan buckets to find max gap
#     max_gap = 0
#     prev = min_val
#     for b_min, b_max in buckets:
#         if b_min == math.inf:  # empty bucket
#             continue
#         max_gap = max(max_gap, b_min - prev)
#         prev = b_max

#     # Final gap with max_val
#     max_gap = max(max_gap, max_val - prev)

#     return max_gap


# import math
# from typing import List
# class Solution:
#     def maximumGap(self, nums: List[int]) -> int:
#         if len(nums) < 2:
#             return 0
#         max_val = max(nums)
#         min_val = min(nums)
#         n = len(nums)
#         bucket_size = math.ceil((max_val - min_val) / (n-1))
#         #how many buckets do we need to cover the whole range [min_val, max_val]?
# #         Suppose we want to place max_val into some bucket.
# # If (max_val - min_val) is exactly divisible by bucket_size, we still need one more bucket to hold max_val.
# # So we add +1 to ensure the last bucket always exists.
#         num_buckets = (max_val - min_val) // bucket_size + 1 
#         #[[min, max]], but initialize with [inf, -inf]
#         buckets = [[math.inf, -math.inf] for _ in range(num_buckets)]
#         for num in nums:
#             if num == max_val or num == min_val:
#                 continue
#             #how do we know which buckets `num` goes into? 
#             # compute index by subtracting the min_val, then see which bucket it goes in
#             index = (num - min_val) // bucket_size
#             buckets[index][0] = min(buckets[index][0], num) #store min
#             buckets[index][1] = max(buckets[index][1], num) #store max
#         max_gap = 0
#         prev_max = 0
#         for i in range(num_buckets):
#             if buckets[i][0] == math.inf or buckets[i][1] == -math.inf:
#                 continue
#             curr_min = buckets[i][0] #min
#             max_gap = max(max_gap, curr_min - prev_max)
#             prev_max = buckets[i][1]
#         max_gap = max(max_gap, max_val - prev_max) #tricky, check last number
#         return max_gap

# q = Solution()
# nums = [1,100000]
# print(q.maximumGap(nums))

# print(maximumGap(nums))

# from typing import List
# from collections import Counter
# class Solution:
#     def findLeastNumOfUniqueInts(self, arr: List[int], k: int) -> int:
#         # counts = Counter(arr)
#         # unique_remaining = len(counts)

#         # # [2,2,2,2,5,5,5,5] becomes [4,4]
#         # for freq in sorted(counts.values()): # go thru [4,4]
#         #     if k >= freq:
#         #         k -= freq
#         #         unique_remaining -= 1
#         #     else:
#         #         return unique_remaining
#         # return unique_remaining

#         c = Counter(arr)
#         cnt, remaining = Counter(c.values()), len(c)
#         print(cnt)
#         for key in sorted(cnt): 
#             if k >= key * cnt[key]:
#                 k -= key * cnt[key]
#                 remaining -= cnt.pop(key)
#             else:
#                 return remaining - k // key
#         return remaining

# arr = [4,3,1,1,3,3,2]
# q=  Solution()
# q.findLeastNumOfUniqueInts(arr,3)


