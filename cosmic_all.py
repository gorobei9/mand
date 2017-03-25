
from dbo import _DBO
from graph import node
from type_registry import _tr
from context import Context

class CosmicAll(_DBO):
    """The cosmic all is the one object in the system that always lives in the root context."""

    _isCosmic = True

    @node
    def dbState(self):
        return None

    def str(self):
        return '<CosmicAll: %s>' % self.meta.name()

    def _wroteEvent(self):
        node = Context._root().getFromBM(self.dbState)
        node._invalidate()

_tr.add(CosmicAll)
