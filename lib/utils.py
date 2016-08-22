import heapq


def takeOrderedByKey(self, num, sortValue=None, reverse=False):

    def init(a):
        return [a]

    def combine(agg, a):
        agg.append(a)
        return getTopN(agg)

    def merge(a, b):
        agg = a + b
        return getTopN(agg)

    def getTopN(agg):
        if reverse:
            return heapq.nlargest(num, agg, sortValue)
        else:
            return heapq.nsmallest(num, agg, sortValue)

    return self.combineByKey(init, combine, merge)
