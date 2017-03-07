
from noval import _noVal

class Context(object):
    _contexts = []
    
    def __init__(self, tweaks):
        p = self.parent()
        self.tweaks = p.tweaks.copy() if p else {}
        self.tweaks.update(tweaks)
        self.cache = self.tweaks.copy()
    def __enter__(self, *a):
        self._contexts.append(self)
    def __exit__(self, *a):
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
    
