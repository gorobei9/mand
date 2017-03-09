
from context import Context
from noval import _noVal
from monitor import Monitor

stack = [] # nodes being computed in standard Python


def addDep(dst, src):
    def p(t):
        return '%s@%x/%s' % (t[0].__class__.__name__, id(t[0]), t[1])
    print 'adding dep on', p(src)
    print '          for', p(dst)
    
def getValue(f, fName, a, k):
    # XXX - this doesn't handle methods with args correctly
    obj = a[0]
    # print 'GET:', f
    name = f.func_name
    key = getattr(obj, name)
    mkey = (obj, fName)

    #if stack:
    #    addDep(stack[-1], mkey)        
    stack.append(mkey)

    try:
        ctx = Context.current()
        Monitor.msg('GetValue', 1, 'begin', key=key, ctx=ctx)
        v = ctx.get(key)
        if v is not _noVal:
            Monitor.msg('GetValue', -1, 'from ctx', key=key, ctx=ctx, value=v)
            return v
        v = obj.meta.getField(name)
        if v is not _noVal:
            Monitor.msg('GetValue', -1, 'from stored', key=key, ctx=ctx, value=v)
            return v
        Monitor.msg('GetValue/Calc', 1, 'begin', mkey=key, ctx=ctx)
        v = f(*a, **k)
        Monitor.msg('GetValue/Calc', -1, 'end', mkey=key, ctx=ctx)
        if name in obj._storedFields():
            Monitor.msg('SetStored', 0, 'set', key=key, value=v)
            obj.meta.setField(name, v)
        ctx.set(key, v)

        Monitor.msg('GetValue', -1, 'end', key=key, ctx=ctx, value=v)
        return v

    finally:
        stack.pop()

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
