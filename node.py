
from noval import _noVal

class Node(object):
    def __init__(self, ctx, key, value, tweakPoint=None, tweakable=False):
        self.ctx = ctx
        self.value = value
        self.key = key
        self.tweakable = tweakable
        self.tweakPoint = tweakPoint # XXX - fix this mess
        self.inputs = set()
        self.outputs = set() # hardly pulling its weight: only used by _invalidate
        self.footnotes = {}

    def object(self):
        return self.key[0]

    def methodId(self):
        return self.key[1]
    
    def __repr__(self):
        key = self.key
        ctx = self.ctx
        return '<%s.(%s) in %s>' % (key[0].meta.path(), key[1], ctx.name)
        #return '<%s@%x/%s in %s>' % (key[0].__class__.__name__, id(key[0]), key[1], ctx.name)

    def find(self, fn):
        ret = set()
        def _find(node):
            if fn(node):
                ret.add(node)
            else:
                for i in node.inputs:
                    _find(i)
        _find(self)
        return list(ret)
    
    def printInputGraph(self, depth=0):
        if self.value is _noVal:
            print '  '*depth, self, '*not evaluated*'
        else:
            print '  '*depth, self
        for i in self.inputs:
            i.printInputGraph(depth+1)

    def _invalidate(self):
        if self.value is not _noVal:
            self.value = _noVal
            for o in self.outputs:
                o._invalidate()
            self.inputs = set()
            self.outputs = set()
