import heapq
from collections import Counter

arr = [4,3,1,1,3,3,2]
freqs = list(Counter(arr).values())
print(freqs)

heapq.heapify(freqs) 
#min heap by default in python
#C++ PQ is max heap by default
print(freqs)
# while freqs:
#     print(freqs[0]) #same as C++ freqs.top()
#     print(heapq.heappop(freqs))

# class Solution:
#     def findLeastNumOfUniqueInts(self, arr: List[int], k: int) -> int:
# #solution 2, use PQ / heapq
#         min_heap = list(Counter(arr).values()) #store freqs only
#         heapq.heapify(min_heap)
#         while k>0:
#             k -= heapq.heappop(min_heap) # pop and subtract
#         return len(min_heap) + (k < 0) #if k is <0, add that popped one back
