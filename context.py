
from noval import _noVal
from monitor import Monitor

class Context(object):
    _contexts = []
    
    def __init__(self, tweaks, name=None):
        p = self.parent()
        self.tweaks = p.tweaks.copy() if p else {}
        self.tweaks.update(tweaks)
        self.cache = self.tweaks.copy()
        self.name = name if name else '%s' % id(name)
        if p:
            self.name = '%s:%s' % (p.name, self.name)
        
    def __enter__(self, *a):
        Monitor.msg('Context', 1, 'enter', ctx=self)
        self._contexts.append(self)
    def __exit__(self, *a):
        Monitor.msg('Context', -1, 'exit', ctx=self)
        c = self._contexts.pop()
        assert c == self
    def get(self, cmb):
        return self.cache.get(cmb, _noVal)
    def set(self, cmb, v):
        self.cache[cmb] = v
  
    @classmethod
    def current(cls):
        if not cls._contexts:
            cls._contexts.append(Context({}))
        return cls._contexts[-1]
    
    @classmethod
    def parent(cls):
        if len(cls._contexts) > 1:
            return cls._contexts[-2]
    
    @classmethod
    def inRootContext(cls):
        return cls.current() == cls._contexts[0]
    
