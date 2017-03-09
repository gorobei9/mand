
from context import Context
from noval import _noVal

stack = []

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
    dkey = (obj, fName)

    #if stack:
    #    addDep(stack[-1], dkey)        
    stack.append(dkey)

    try:
            
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
            #fn.func_name = f.func_name
            return fn
        return g
    
    f = a[0]
    info = {'name': f.func_name}
    def fn(*aa, **kk):
        v = getValue(f, info['key'], aa, kk)
        return v
    fn.nodeInfo = info
    #fn.func_name = f.func_name
    return fn
