
from noval import _noVal

class NodeKey(object):
    
    def __init__(self, obj, bm, fullName, args):
        assert ':' in fullName
        self._key = (obj, fullName, args)
        self.bm = bm

    def object(self):
        return self._key[0]
    
    def fullName(self):
        return self._key[1]
    
    def __repr__(self):
        return '<NodeKey:%s.(%s) %s %s>' % (self._key + (self.fn,))

    def strForMonitor(self):
        key = self._key
        return '%s@%x/%s:%s' % (key[0].__class__.__name__, id(key[0]), key[1], key[2])
    
    @classmethod
    def fromBM(cls, bm):
        obj = bm.im_self
        nodeInfo = bm.nodeInfo
        fullName = nodeInfo['key']
        args = ()
        return NodeKey(obj, bm, fullName, args)
        
class Node(object):
    def __init__(self, ctx, key, value): # , tweakPoint=None, tweakable=False, onCalced=None):
        self.ctx = ctx
        self.value = value
        self.key = key
        self.tweaked = False
        self.tweakable = False
        self.inputs = set()
        self.outputs = set() # hardly pulling its weight: only used by _invalidate
        self.footnotes = {}
        self.tweakPoint = key.bm
        
    def copy(self, newCtx):
        c = Node(newCtx, self.key, self.value)
        c.tweakable = self.tweakable
        c.tweakPoint = self.tweakPoint
        return c
        
    def object(self):
        return self.key.object()

    def methodId(self):
        return self.key.fullName()
    
    def __repr__(self):
        key = self.key
        ctx = self.ctx
        return '<%s in %s>' % (key.strForMonitor(), ctx.name)

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
            print '   '*depth, self, '*not evaluated*'
        else:
            print'%s%s, nIn=%s' % ('   '*depth, self, len(self.inputs))
        for i in sorted(self.inputs, key=lambda n: [ n.object().meta.path(), n.methodId() ]):
            i.printInputGraph(depth+1)

    def _invalidate(self):
        if self.value is not _noVal:
            self.value = _noVal
            for o in self.outputs:
                o._invalidate()
            self.inputs = set()
            self.outputs = set()
