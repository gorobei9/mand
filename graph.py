
from context import Context
from noval import _noVal

def getValue(f, a, k):
    # XXX - this doesn't handle methods with args correctly
    obj = a[0]
    name = f.func_name
    key = getattr(obj, name)
    ctx = Context.current()
    v = ctx.get(key)
    if v is not _noVal:
        return v
    v = obj.meta.getField(name)
    if v is not _noVal:
        return v
    v = f(*a, **k)
    if name in obj._storedFields():
        obj.meta.setField(name, v)
    ctx.set(key, v)
    
    return v


def node(*a, **k):
    # XXX - this doesn't handle methods with args correctly
    if k:
        def g(*aa, **kk):
            for kw in k:
                assert kw in ('stored',)
            f = aa[0]
            info = k.copy()
            info['name'] = f.func_name
            def fn2(*aaa, **kkk):
                v = getValue(f, aaa, kkk)
                return v
            fn2.nodeInfo = info
            return fn2
        return g
    
    f = a[0]
    def fn(*aa, **kk):
        v = getValue(f, aa, kk)
        return v
    fn.nodeInfo = {'name': f.func_name}
    return fn
