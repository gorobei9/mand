
from noval import _noVal
from monitor import Monitor
from node import Node

class ContextBase(object):

    def getCBM(self, k):
        key = (k.im_self, k.nodeInfo['key'])
        node = self.getNode(key)
        return node
    
    def getNode(self, key):
        v = self.get(key)
        if v:
            return v
        else:
            node = Node(self, key, value=_noVal)
            self.cache[key] = node
            return node
            
    def get(self, key):
        return self.cache.get(key)
        
    def set(self, key, node):
        Monitor.msg('Context', 0, 'set', ctx=self, key=key, value=node.value)
        self.cache[key] = node
        return node
    
class RootContext(ContextBase):
    def __init__(self):
        self.parent = None
        self.tweaks = {}
        self.cache = {}
        self.name = 'Root'

class Context(ContextBase):
    _contexts = [RootContext()]
    
    def __init__(self, tweaks, name=None):
        p = self.current()

        self.name = name if name else '%s' % id(name)
        if p:
            self.name = '%s:%s' % (p.name, self.name)
        Monitor.msg('Context', 0, 'create', ctx=self)

        self.tweaks = p.tweaks.copy() if p else {}
        self.tweaks.update(tweaks)
        
        self.cache = {}
        for k, v in self.tweaks.items():
            key = (k.im_self, k.nodeInfo['key'])
            self.set(key, Node(self, key, v))

    def __enter__(self, *a):
        Monitor.msg('Context', 1, 'enter', ctx=self)
        self._contexts.append(self)
        
    def __exit__(self, *a):
        Monitor.msg('Context', -1, 'exit', ctx=self)
        c = self._contexts.pop()
        assert c == self
      
    @classmethod
    def current(cls):
        return cls._contexts[-1]
    
    @classmethod
    def _root(cls):
        return cls._contexts[0]

    @classmethod
    def inRootContext(cls):
        return cls.current() == cls._contexts[0]
    
