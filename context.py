
from noval import _noVal
from monitor import Monitor
from node import Node

class ContextBase(object):

    def getBM(self, k):
        key = (k.im_self, k.nodeInfo['key'])
        node = self.getNode(key, boundMethod=k)
        return node
    
    def _getNode(self, key):
        return self.get(key)
    
    def getNode(self, key, boundMethod):
        v = self.get(key)
        if v:
            return v
        else:
            tweakable = boundMethod.nodeInfo.get('tweakable', False)
            tweakPoint = boundMethod
            node = Node(self, key, value=_noVal, tweakPoint=tweakPoint, tweakable=tweakable)
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
    
    def __init__(self, tweaks, name=None, parented=True):
        p = self.current() if parented else None

        self.name = name if name else '%s' % id(name)
        if p:
            self.name = '%s:%s' % (p.name, self.name)
        Monitor.msg('Context', 0, 'create', ctx=self, name=self.name)

        allTweaks = p.tweaks.copy() if p else {}
        allTweaks.update(tweaks)

        self.tweaks = set()
        self.cache = {}
        for k, v in allTweaks.items():
            node = self.getBM(k)
            key = node.key
            if not node.tweakable:
                raise RuntimeError('trying to tweak un-tweakable %s' % node)
            node.value = v
            node.tweaked = True
            self.tweaks.add(node)
            self.set(key, node)

    def __enter__(self, *a):
        Monitor.msg('Context', 1, 'enter', ctx=self, name=self.name)
        self._contexts.append(self)
        
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
    
