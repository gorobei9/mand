
from printutils import strForm
from utils import displayListOfDicts, displayMarkdown
import time

class Monitor(object):
    monitors = []

    def __enter__(self):
        self.monitors.append(self)

    def __exit__(self, *a):
        self.monitors.pop()
        if a:
            self.onExitErr()
        else:
            self.onExit()

    def onExitErr(self):
        self.onExit()
        
    def keyStr(self, key):
        return '%s@%x/%s' % (key[0].__class__.__name__, id(key[0]), key[1])

    def ctxStr(self, ctx):
        return ctx.name
    
    def kwToStr(self, kw):
        strs = []
        def addStr(k, f):
            if k in kw:
                v = kw.pop(k)
                strs.append('%s: %s' % (k, f(v)))
        addStr('key',     lambda v: self.keyStr(v))
        addStr('path',    lambda v: strForm(v, 80))
        addStr('value',   lambda v: strForm(v, 40))
        addStr('url',     lambda v: strForm(v, 80))
        addStr('ctx',     lambda v: self.ctxStr(v))
        addStr('metaobj', lambda v: v.path())
        addStr('obj',     lambda v: v.meta.path())
        if kw:
            strs.append('other: %s' % kw.keys())
        info = ', '.join(strs)
        return info
    
    def message(self, *a, **k):
        print 'Monitor(baseclass).msg:', a, k

    def onExit(self):
        pass
    
    @classmethod
    def msg(cls, *a, **k):
        for monitor in cls.monitors:
            monitor.message(*a, **k)

class PrintMonitor(Monitor):
    
    def __init__(self, include=None):
        self.depth = 0
        self.include = include

    def message(self, sys, depthInc, action, **kw):
        depth = self.depth
        ind = '  '*depth
        self.depth += depthInc

        if self.include is not None and sys not in self.include:
            return
        
        info = self.kwToStr(kw)

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
        
class ProfileMonitor(Monitor):

    def __init__(self, mode=None):
        self.stack = []
        self.result = []
        self.mode = mode
        
    def message(self, sys, depthInc, action, **kw):
        if depthInc == 1:
            v = [kw, time.time(), 0]
            self.stack.append(v)
        if depthInc == -1:
            kw, start, tSub = self.stack.pop()
            end = time.time()
            t = end - start               # total time in this block
            tFn = t - tSub                # time in block - time in sub-blocks
            if self.stack:
                self.stack[-1][-1] += t   # remove my total time from parent block
            
            self.result.append((tFn, t, sys, kw))
             
    def dumpRaw(self):
        for tFn, t, sys, kw in self.result:
            key = self.kwToStr(kw)
            print '%8.4f %8.4f %-16s: %s' % (tFn, t, sys, key)
            
    def displaySum(self, check=True):
        if check and self.stack:
            print 'Monitor: Stuff still on stack:', self.stack
            assert False
        if not self.result:
            displayMarkdown('No profile info was recorded.')
        n = {}
        cumT = {}
        cumTCalc = {}
        tScale = 1e6
        for tFn, t, sys, kw in self.result:
            if 'path' in kw:
                fn = kw.get('path', '//').split('/')[2]
            elif 'name' in kw:
                fn = kw['name']
            else:
                fn = kw.get('key', (None, ''))[1]
            key = (sys, fn)
            cumT[key]     = cumT.get(key, 0) + t * tScale
            cumTCalc[key] = cumTCalc.get(key, 0) + tFn * tScale
            n[key]        = n.get(key, 0) + 1
        res = []
        def f(d):
            return format(int(d), ',d')
        for key in n:
            line = { 'n':         f(n[key]), 
                     'cumT':      f(cumT[key]),
                     'cumT/call': f(cumT[key]/n[key]),
                     'calcT':     f(cumTCalc[key]),
                     'sys':       key[0],
                     'fn':        key[1],
                     'key':       cumT[key],
                     }
            res.append(line)
        res = sorted(res, key=lambda d: -d['key'])
        txt = """
### Profile by nodes.
* times are in microseconds
* cumT is total time spent in funtion
* calcT is time spent in function, but not in a child node"""
        displayMarkdown(txt)
        displayListOfDicts(res, names=['fn', 'n', 'cumT', 'calcT', 'cumT/call', 'sys'])

    def onExitErr(self):
        self.onExit(check=False)
        
    def onExit(self, check=True):
        mode = self.mode
        if mode is None:
            self.dumpRaw()
        elif mode == 'sum':
            self.displaySum(check=check)
        else:
            assert False, 'unknown profiler mode: %s' % mode
            
