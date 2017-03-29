
from context import Context
from noval import _noVal
from monitor import Monitor
from node import Node, NodeKey
from depmanager import DependencyManager, dm, getNode
    
                 
def _getCurrentNode():
    return dm().stack[-1]

def find(bm, fn):
    v = bm()
    n = getNode(bm)
    return n.find(fn)
    
def getValue(f, key):
    obj = key.object()
    ctx = Context._root() if obj._isCosmic else Context.current()

    _dm = dm()
    
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
        if _dm.calculated(node):
            node.value = v
        #else:
        #    print 'huh? calc fail:', node
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
