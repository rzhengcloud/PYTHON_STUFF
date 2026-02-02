class Solution:
    def findTheDifference(self, s: str, t: str) -> str:
        # ms = Counter(s)
        # mt = Counter(t)

        # for c in t:
        #     if mt[c] - ms[c] == 1:
        #         return c

        # ms, mt = defaultdict(int), defaultdict(int)
        # for c in s:
        #     ms[c]+=1
        # for c in t:
        #     mt[c]+=1
        # for c in t:
        #     if mt[c] - ms[c] == 1:
        #         return c

        ms, mt = {}, {}
        for c in s:
            ms[c] = ms.get(c,0) + 1
        for c in t:
            mt[c] = mt.get(c,0) + 1
        for c in t:
            if mt.get(c,0) - ms.get(c,0) == 1:
                return c