
from context import Context
from noval import _noVal
from monitor import Monitor
from node import Node, NodeKey
from dictutils import merge

class DependencyManager(object):
    def __init__(self):
        self.stack = []
        
    # Input management stuff...
    
    def establishDep(self):
        if len(self.stack) > 1:
            output = self.stack[-2]
            input = self.stack[-1]
            self.addDep(input, output)
            self.mergeMeta(input, output)
            
    def mergeMeta(self, input, output):
        merge(output.footnotes, input.footnotes, deleteZeros=False)
        output._tweakPoints.update(input.tweakPoints())

    def addDep(self, input, output):
        output.inputs.add(input)
        input.outputs.add(output)

    def push(self, node):
        self.stack.append(node)
        
    def pop(self):
        self.stack.pop()

    # Context selection stuff...

    def calculated(self, node):
        pass
    
    def getNode(self, ctx, key):
        node = ctx.get(key)
        return node
    
_dm = DependencyManager()

def setDependencyManager(dm):
    global _dm
    _dm = dm

                 
def _getCurrentNode():
    return _dm.stack[-1]

def getNode(bm):
    obj = bm.im_self
    if obj._isCosmic:
        ctx = Context._root()
    else:
        ctx = Context.current()
    key = NodeKey.fromBM(bm)
    node = _dm.getNode(ctx, key)
    return node

def find(bm, fn):
    v = bm()
    n = getNode(bm)
    return n.find(fn)
    
def getValue(f, key):
    obj = key.object()
    ctx = Context._root() if obj._isCosmic else Context.current()

    node = _dm.getNode(ctx, key)
    
    _dm.push(node)

    try:
        Monitor.msg('GetValue', 1, 'begin', key=key, ctx=ctx)
        v = node.value
        if v is not _noVal:
            Monitor.msg('GetValue', -1, 'from ctx', key=key, ctx=ctx, value=v)
            return v
        name = f.func_name
        v = obj.meta.getField(name)
        if v is not _noVal:
            Monitor.msg('GetValue', -1, 'from stored', key=key, ctx=ctx, value=v)
            node.value = v
            return v
        ctxE = node.ctx
        Monitor.msg('GetValue/Calc', 1, 'begin', key=key, ctx=ctxE)
        with ctxE:
            fa = key.fArgs()
            v = f(*fa)
        Monitor.msg('GetValue/Calc', -1, 'end', key=key, ctx=ctxE)
        if name in obj._storedFields():
            Monitor.msg('SetStored', 0, 'set', key=key, value=v)
            obj.meta.setField(name, v)
        node.value = v
        _dm.calculated(node)
        Monitor.msg('GetValue', -1, 'end', key=key, ctx=ctx, value=v)
        return v

    finally:
        _dm.establishDep()
        _dm.pop()

def getKey(f, info, a, k):
    assert not k
    fName = info['key']
    tweakable = info.get('tweakable', False)
    # XXX - this doesn't handle methods with kwargs correctly
    obj = a[0]
    args = a[1:]
    name = f.func_name
    key = NodeKey(obj, f, fName, args, tweakable)
    return key

def makeFn(f, info={}):
    name = f.func_name
    info = info.copy()
    info['name'] =  name
    # Note: info['key'] is added by DBOMetaClass
    def fn(*a, **k):
        key = getKey(f, info, a, k)
        v = getValue(f, key)
        return v
    fn.nodeInfo = info
    return fn
    
def node(*a, **k):
    # XXX - this doesn't handle methods with args correctly
    if k:
        def g(*aa, **kk):
            for kw in k:
                assert kw in ('stored', 'tweakable')
            f = aa[0]
            return makeFn(f, info=k)
        return g
    
    f = a[0]
    return makeFn(f)
