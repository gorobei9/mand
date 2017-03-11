
from printutils import strForm

class Monitor(object):
    monitors = []

    def __enter__(self):
        self.monitors.append(self)

    def __exit__(self, *a):
        self.monitors.pop()
        self.onExit()

    def keyStr(self, key):
        return '%s@%x/%s' % (key[0].__class__.__name__, id(key[0]), key[1])

    def ctxStr(self, ctx):
        return ctx.name
    
    def message(self, *a, **k):
        print 'Monitor(baseclass).msg:', a, k

    def onExit(self):
        pass
    
    @classmethod
    def msg(cls, *a, **k):
        for monitor in cls.monitors:
            monitor.message(*a, **k)

class PrintMonitor(Monitor):
    
    def __init__(self):
        self.depth = 0
        
    def message(self, sys, depthInc, action, **kw):
        depth = self.depth
        ind = '  '*depth
        self.depth += depthInc

        strs = []
        def addStr(k, f):
            if k in kw:
                v = kw.pop(k)
                strs.append('%s: %s' % (k, f(v)))
        addStr('value',   lambda v: strForm(v, 20))
        addStr('ctx',     lambda v: self.ctxStr(v))
        addStr('metaobj', lambda v: v.path())
        addStr('obj',     lambda v: v.meta.path())
        addStr('key',     lambda v: self.keyStr(v))
        if kw:
            strs.append('other: %s' % kw.keys())
        info = ', '.join(strs)
        
        if sys in ('GetValue', 'GetValue/Calc'):
            print ind, sys, action, info
            return
        if action in ('end', 'exit'):
            return
        print ind, sys, action, info
            
class SummaryMonitor(Monitor):

    def __init__(self):
        self.counts = {}
        
    def message(self, sys, depthInc, action, **kw):
        if action in ('begin', 'create'):
            self.counts[sys] = self.counts.get(sys, 0) + 1

    def onExit(self):
        print 'Compute activity:'
        for i in sorted(self.counts.items()):
            print '  %20s: %5d' % i
        

    
