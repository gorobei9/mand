
from dbo import _DBO
from graph import node
from type_registry import _tr
from context import Context

class CosmicAll(_DBO):
    @node
    def dbState(self):
        return None

    def str(self):
        return '<CosmicAll: %s>' % self.meta.name()

    def _wroteEvent(self):
        node = Context.current().getCBM(self.dbState)
        node._invalidate()

_tr.add(CosmicAll)
