
class Monitor(object):
    monitors = []

    def __enter__(self):
        self.monitors.append(self)

    def __exit__(self, *a):
        self.monitors.pop()

    def keyStr(self, key):
        return '<%s>.%s()' % (key.im_self.meta.path(), key.nodeInfo['name'])

    def ctxStr(self, ctx):
        return ctx.name
    
    def message(self, *a, **k):
        print 'Monitor(baseclass).msg:', a, k
        
    @classmethod
    def msg(cls, *a, **k):
        for monitor in cls.monitors:
            monitor.message(*a, **k)

class PrintMonitor(Monitor):
    
    def __init__(self):
        self.depth = 0
        
    def message(self, sys, depthInc, action, **kw):
        depth = self.depth
        self.depth += depthInc

        if action in ('end', 'exit'):
            return
        
        if sys=='DB':
            obj = kw['obj']
            print '  '* depth, sys, action, obj.path()
        elif sys=='GetValue':
            key = kw['key']
            print '  '* depth, sys, action, self.keyStr(key)
        elif sys=='GetValue/Calc':
            key = kw['mkey'].nodeInfo['key']
            print '  '* depth, sys, action, key
        elif sys=='Context':
            ctx = kw['ctx']
            print '  '* depth, sys, action, self.ctxStr(ctx)
        else:  
            print '  '* depth, sys, action, kw.keys()
            
