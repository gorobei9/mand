
from dbo import _DBO
from graph import node
from type_registry import _tr

class CosmicAll(_DBO):
    @node
    def dbState(self):
        return None

    def str(self):
        return '<CosmicAll: %s>' % self.meta.name()

_tr.add(CosmicAll)
