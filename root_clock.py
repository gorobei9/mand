
from dbo import _DBO
from graph import node
from type_registry import _tr
from cosmic_all import CosmicAll

class RootClock(_DBO):
    @node
    def cosmicAll(self):
        return CosmicAll.get('TheCosmicAll', db=self.meta.db)
    
    @node(tweakable=True)
    def cutoffs(self):
        self.cosmicAll().dbState()
        return None
    
    @node
    def parent(self):
        return None

    def str(self):
        return '<RootClock: %s>' % self.meta.path()

_tr.add(RootClock)
