
from .noval import _noVal

class NodeKey(object):
    
    def __init__(self, obj, fn, fullName, args, tweakable):
        assert ':' in fullName
        self._key = (obj, fullName, args)
        self.tweakable = tweakable
         
    def fArgs(self):
        return (self.object(),) + self._key[2]
    
    def object(self):
        return self._key[0]
    
    def fullName(self):
        return self._key[1]

    def shortName(self):
        return self.fullName().split(':')[-1]

    def __repr__(self):
        return '<NodeKey:%s.(%s) %s>' % self._key

    def strForMonitor(self):
        key = self._key
        return '%s.(%s)%s' % (key[0].meta.path(), key[1], key[2])
    
    @classmethod
    def fromBM(cls, bm):
        obj = bm.__self__ # im_self
        nodeInfo = bm.nodeInfo
        fullName = nodeInfo['key']
        tweakable = nodeInfo.get('tweakable', False)
        args = ()
        return NodeKey(obj, bm, fullName, args, tweakable)
        
class Node(object):
    def __init__(self, ctx, key, value):
        self.ctx = ctx
        self.value = value
        self.key = key
        self.tweaked = False
        self.inputs = set()
        self.outputs = set() # hardly pulling its weight: only used by _invalidate
        
        self.simplified = False
        
        self.footnotes = {}
        self._tweakPoints = set()

    def floatingTweakPoints(self):
        if self.tweaked:
            return set()
        elif self.key.tweakable:
            return set([self])
        else:
            return self._tweakPoints
        
    def copy(self, newCtx):
        c = Node(newCtx, self.key, self.value)
        return c

    def tweak(self, v):
        if not self.key.tweakable:
            raise RuntimeError('trying to tweak un-tweakable %s' % self)
        self.value = v
        self.tweaked = True

    def object(self):
        return self.key.object()

    def methodId(self):
        return self.key.fullName()
    
    def __repr__(self):
        key = self.key
        ctx = self.ctx
        return '<Node: %s in %s @%s>' % (key.strForMonitor(), ctx.name, id(self))

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
    
    def printInputGraph(self, depth=0, maxDepth=None):
        if maxDepth is not None and depth >= maxDepth:
            return
        if self.value is _noVal:
            print('  '*depth, self, '*not evaluated*')
        else:
            print('%s%s, nIn=%s' % ('   '*depth, self, len(self.inputs)))
        for i, n in enumerate(sorted(self.inputs, key=lambda n: [ n.object().meta.path(), n.methodId() ])):
            if maxDepth is not None and i > 10:
                print('   '*depth, '...')
                break
            n.printInputGraph(depth+1, maxDepth=maxDepth)

    def _invalidate(self):
        if self.value is not _noVal:
            self.value = _noVal
            for o in self.outputs:
                o._invalidate()
            self.inputs = set()
            self.outputs = set()

    def footnoteMarkdown(self):
        ret = []
        for k, i in self.footnotes.items():
            ret.append( '1. %s' % k)
            for v in sorted(i):
                ret.append( '  * %s' % v)
        return '\n'.join(ret)
