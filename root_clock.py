
from dbo import _DBO
from graph import node
from type_registry import _tr

class RootClock(_DBO):
    @node
    def cutoffs(self):
        return None
    
    @node
    def parent(self):
        return None

    def str(self):
        return '<RootClock: %s>' % self.meta.path()

_tr.add(RootClock)
