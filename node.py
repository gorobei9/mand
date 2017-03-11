
from noval import _noVal

class Node(object):
    def __init__(self, ctx, key, value):
        self.ctx = ctx
        self.value = value
        self.key = key
        self.inputs = set()
        self.outputs = set()

    def __repr__(self):
        key = self.key
        ctx = self.ctx
        return '<%s@%x/%s in %s>' % (key[0].__class__.__name__, id(key[0]), key[1], ctx.name)

    def printInputGraph(self, depth=0):
        print '  '*depth, self
        for i in self.inputs:
            i.printInputGraph(depth+1)

    def _invalidate(self):
        self.value = _noVal
        self.inputs = set()
        self.outputs = set()
