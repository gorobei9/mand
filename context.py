
from noval import _noVal
from monitor import Monitor

class RootContext(object):
    def __init__(self):
        self.parent = None
        self.tweaks = {}
        self.cache = {}
        self.name = 'Root'
    def get(self, key):
        return self.cache.get(key, _noVal)
    def set(self, key, v):
        Monitor.msg('Context', 0, 'set', ctx=self, key=key, value=v)
        self.cache[key] = v

class Context(object):
    _contexts = [RootContext()]
    
    def __init__(self, tweaks, name=None):
        p = self.current()
        self.tweaks = p.tweaks.copy() if p else {}
        self.tweaks.update(tweaks)
        
        cache = {}
        for k, v in self.tweaks.items():
            key = (k.im_self, k.nodeInfo['key'])
            cache[key] = v
            
        self.name = name if name else '%s' % id(name)
        if p:
            self.name = '%s:%s' % (p.name, self.name)
        Monitor.msg('Context', 0, 'create', ctx=self)
        self.cache = cache
        
    def __enter__(self, *a):
        Monitor.msg('Context', 1, 'enter', ctx=self)
        self._contexts.append(self)
    def __exit__(self, *a):
        Monitor.msg('Context', -1, 'exit', ctx=self)
        c = self._contexts.pop()
        assert c == self
    def get(self, key):
        return self.cache.get(key, _noVal)
    def set(self, key, v):
        Monitor.msg('Context', 0, 'set', ctx=self, key=key, value=v)
        self.cache[key] = v
  
    @classmethod
    def current(cls):
        return cls._contexts[-1]
    
    @classmethod
    def inRootContext(cls):
        return cls.current() == cls._contexts[0]
    
