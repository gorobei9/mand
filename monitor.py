
from .printutils import strForm
from .utils import displayListOfDicts, displayMarkdown, displayDict, displayHeader
import time

class Monitor(object):
    monitors = []

    def __enter__(self):
        self.monitors.append(self)
        self.onEntry()
        
    def __exit__(self, *a):
        self.monitors.pop()
        if a:
            self.onExitErr()
        else:
            self.onExit()
        return False

    def onEntry(self):
        pass
    
    def onExit(self):
        pass
    
    def onExitErr(self):
        self.onExit()
        
    def keyStr(self, key):
        return key.strForMonitor()

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
        addStr('url',     lambda v: strForm(v, 80))
        addStr('ctx',     lambda v: self.ctxStr(v))
        addStr('metaobj', lambda v: v.path())
        addStr('obj',     lambda v: v.meta.path())
        addStr('value',   lambda v: strForm(v, 40))
        if kw:
            strs.append('other: %s' % kw.keys())
        info = ', '.join(strs)
        return info
    
    def message(self, *a, **k):
        print('Monitor(baseclass).msg:', a, k)

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
            print(ind, sys, action, info)
            return
        if action in ('end', 'exit'):
            return
        print(ind, sys, action, info)
            
class SummaryMonitor(Monitor):

    def __init__(self):
        self.counts = {}
        
    def onEntry(self):
        self.start = time.time()
    
    def message(self, sys, depthInc, action, **kw):
        if action in ('begin', 'create'):
            self.counts[sys] = self.counts.get(sys, 0) + 1

    def onExit(self):
        elapsed = time.time() - self.start
        displayHeader('Compute activity summary (%.2f seconds of wall clock time)' % elapsed, level=3)
        displayDict(self.counts)
        #for i in sorted(self.counts.items()):
        #    print '  %20s: %5d' % i
        
class ProfileMonitor(Monitor):

    def __init__(self, mode='sum'):
        self.stack = []
        self.result = []
        self.mode = mode
        self.active = {}
        
    def message(self, sys, depthInc, action, **kw):
        if depthInc == 1:
            key = self.key(sys, kw)
            v = [key, time.time(), 0]
            self.stack.append(v)
            self.active[key] = self.active.get(key, 0) + 1
        if depthInc == -1:
            key, start, tSub = self.stack.pop()
            self.active[key] = self.active[key] - 1
            end = time.time()
            t = end - start               # total time in this block
            tFn = t - tSub                # time in block - time in sub-blocks
            if self.stack:
                self.stack[-1][-1] += t   # remove my total time from parent block
            
            if self.active[key] != 0:
                #print 'Recursive profile entry:', key, self.active[key]
                t = 0                     # the cum time will be given to the caller
            self.result.append((tFn, t, key))
             
    def dumpRaw(self):
        for tFn, t, kw in self.result:
            key = self.kwToStr(kw)
            print('%8.4f %8.4f: %s' % (tFn, t, key))

    def key(self, sys, kw):
        if 'path' in kw:
            fn = kw['path'].split('/')[2]
        elif 'name' in kw:
            fn = kw['name']
        else:
            k = kw.get('key')
            fn = k.fullName()
        return (sys, fn)
        
    def displaySum(self, check=True):
        if check and self.stack:
            print('Monitor: Stuff still on stack:', self.stack)
            assert False
        if not self.result:
            displayMarkdown('No profile info was recorded.')
        n = {}
        cumT = {}
        cumTCalc = {}
        tScale = 1e6
        for tFn, t, key in self.result:
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
* cumT is total time spent in function
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
            
