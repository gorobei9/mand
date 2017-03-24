
from context import Context
from noval import _noVal
from monitor import Monitor
from node import Node

class DependencyManager(object):
    def __init__(self):
        self.stack = []
        
    # Input management stuff...
    
    def establishDep(self):
        if len(self.stack) > 1:
            output = self.stack[-2]
            input = self.stack[-1]
            self.addDep(input, output)
            self.addFootnotes(input, output)
            
    def addFootnotes(self, input, output):
        for k, v in input.footnotes.items():
            addFootnote(text=k, infos=v.infos, node=output)
                        
    def addDep(self, input, output):
        output.inputs.add(input)
        input.outputs.add(output)

    def push(self, node):
        self.stack.append(node)
        
    def pop(self):
        self.stack.pop()

    # Context selection stuff...
    
    def getNode(self, ctx, key, bm):
        return ctx.getNode(key, boundMethod=bm)
            
_dm = DependencyManager()

def setDependencyManager(dm):
    global _dm
    _dm = dm

class Footnote(object):
    def __init__(self, text):
        self.text = text
        self.infos = set()
    def addInfo(self, info):
        self.infos.update(info)
    def __repr__(self):
        if self.infos:
            return '%s: %s' % (self.text, ', '.join(sorted(self.infos)))
        else:
            return self.text
        
def addFootnote(text=None,
                info=None,
                infos=None,
                node=None):
    key = text
    if node is None:
        node = _getCurrentNode()
        
    if key in node.footnotes:
        fn = node.footnotes[key]
    else:
        fn = Footnote(text)
        node.footnotes[key] = fn
        
    if info:
        fn.addInfo([info])
    if infos:
        fn.addInfo(infos)
                 
def _getCurrentNode():
    return _dm.stack[-1]

def getNode(bm):
    obj = bm.im_self
    if obj._isCosmic:
        ctx = Context._root()
    else:
        ctx = Context.current()
    node = ctx.getBM(bm)
    return node

def find(bm, fn):
    v = bm()
    n = getNode(bm)
    # XXX - sort this out
    #c = Context.current()
    #n = c.getBM(bm)
    return n.find(fn)
    
def getValue(f, fName, a, k):
    # XXX - this doesn't handle methods with args correctly
    obj = a[0]
    name = f.func_name

    key = (obj, fName)        # the full name of the function we call, possibly a super() method

    if obj._isCosmic:
        ctx = Context._root()
    else:
        ctx = Context.current()
    bm = getattr(obj, name)
    node = _dm.getNode(ctx, key, bm)

    _dm.push(node)

    try:
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
        node.calced()
        Monitor.msg('GetValue/Calc', -1, 'end', key=key, ctx=ctx)
        if name in obj._storedFields():
            Monitor.msg('SetStored', 0, 'set', key=key, value=v)
            obj.meta.setField(name, v)
        node.value = v

        Monitor.msg('GetValue', -1, 'end', key=key, ctx=ctx, value=v)
        return v

    finally:
        _dm.establishDep()
        _dm.pop()

def makeFn(f, name, info={}):
    info = info.copy()
    info['name'] =  name
    def fn(*a, **k):
        v = getValue(f, info['key'], a, k)
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
            return makeFn(f, f.func_name, info=k)
        return g
    
    f = a[0]
    return makeFn(f, f.func_name)
