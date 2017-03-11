
from context import Context
from noval import _noVal
from monitor import Monitor
from node import Node

class DependencyManager(object):
    def __init__(self):
        self.stack = []
        
    def establishDep(self):
        if len(self.stack) > 1:
            output = self.stack[-2]
            input = self.stack[-1]
            output.inputs.add(input)
            input.outputs.add(output)
    def push(self, node):
        self.stack.append(node)
    def pop(self):
        self.stack.pop()
            
_dm = DependencyManager()

def setDependencyManager(dm):
    global _dm
    _dm = dm

    
def getValue(f, fName, a, k):
    # XXX - this doesn't handle methods with args correctly
    obj = a[0]
    name = f.func_name

    # this should be sorted out a bit...
    key = (obj, fName)        # the full name of the function we call, possibly a super() method

    ctx = Context.current()
    node = ctx.getNode(key)

    _dm.push(node)

    try:
        _dm.establishDep()
        Monitor.msg('GetValue', 1, 'begin', key=key, ctx=ctx)
        v = node.value
        if v is not _noVal:
            Monitor.msg('GetValue', -1, 'from ctx', key=key, ctx=ctx, value=v)
            return v
        v = obj.meta.getField(name)
        if v is not _noVal:
            Monitor.msg('GetValue', -1, 'from stored', key=key, ctx=ctx, value=v)
            node.value = v
            return v
        Monitor.msg('GetValue/Calc', 1, 'begin', key=key, ctx=ctx)
        v = f(*a, **k)
        Monitor.msg('GetValue/Calc', -1, 'end', key=key, ctx=ctx)
        if name in obj._storedFields():
            Monitor.msg('SetStored', 0, 'set', key=key, value=v)
            obj.meta.setField(name, v)
        node.value = v

        Monitor.msg('GetValue', -1, 'end', key=key, ctx=ctx, value=v)
        return v

    finally:
        _dm.pop()

def node(*a, **k):
    # XXX - this doesn't handle methods with args correctly
    if k:
        def g(*aa, **kk):
            for kw in k:
                assert kw in ('stored',)
            f = aa[0]
            info = k.copy()
            info['name'] = f.func_name
            def fn(*aaa, **kkk):
                v = getValue(f, info['key'], aaa, kkk)
                return v
            fn.nodeInfo = info
            return fn
        return g
    
    f = a[0]
    info = {'name': f.func_name}
    def fn(*aa, **kk):
        v = getValue(f, info['key'], aa, kk)
        return v
    fn.nodeInfo = info
    return fn
