
class TypeRegistry(object):
    def __init__(self):
        self.clsToName = {}
        self.nameToCls = {}
    def add(self, cls):
        """XXX - Testing method - just register an in-process class with its own name"""
        name = cls.__name__
        self.clsToName[cls] = name
        self.nameToCls[name] = cls
    def cls(self, name):
        return self.nameToCls[name]
    def name(self, cls):
        if cls in self.clsToName:
            return self.clsToName[cls]
        else:
            return 'anon:%s' % cls.__name__
    def __getattr__(self, name):
        return self.cls(name)
    
_tr = TypeRegistry()
