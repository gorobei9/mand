
from noval import _noVal
from monitor import Monitor
from node import Node, NodeKey

class ContextBase(object):

    def _get(self, nodeKey):
        key = nodeKey._key
        return self.cache.get(key)

    def get(self, nodeKey):
        v = self._get(nodeKey)
        if v:
            return v
        node = Node(self, nodeKey, value=_noVal)
        self.set(nodeKey, node)
        return node

    def getFromBM(self, bm): 
        nodeInfo = bm.nodeInfo
        key = NodeKey.fromBM(bm)
        node = self.get(key)
        return node
        
    def set(self, nodeKey, node):
        key = nodeKey._key
        Monitor.msg('Context', 0, 'set', ctx=self, key=nodeKey, value=node.value)
        self.cache[key] = node
        return node
    
class RootContext(ContextBase):
    def __init__(self):
        self.tweaks = set()
        self.cache = {}
        self.name = 'Root'

    def __enter__(self, *a):
        return self
    
    def __exit__(self, *a):
        pass
        
class Context(ContextBase):
    _contexts = [RootContext()]
    
    def __init__(self, tweaks, name=None):
        p = self.current()
        
        self.name = name if name else '%s' % id(name)
        if p:
            self.name = '%s:%s' % (p.name, self.name)
        Monitor.msg('Context', 0, 'create', ctx=self, name=self.name)

        self.allTweaks = set()
        self.tweaks = set()
        self.cache = {}
        
        for node in p.tweaks:
            self.set(node.key, node.copy(self))
            self.allTweaks.add(node)
            
        for k, v in tweaks.items():
            if isinstance(k, Node):
                node = k
            else:
                node = self.getFromBM(k)
            key = node.key
            node.tweak(v)
            self.tweaks.add(node)
            self.set(key, node)

    def __enter__(self, *a):
        Monitor.msg('Context', 1, 'enter', ctx=self, name=self.name)
        self._contexts.append(self)
        return self
    
    def __exit__(self, *a):
        Monitor.msg('Context', -1, 'exit', ctx=self, name=self.name)
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



