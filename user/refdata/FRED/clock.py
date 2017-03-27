
from mand.core import _DBO, node, _tr, CosmicAll
import datetime

class FredTimestamp(object):
    def __init__(self, t=None, v=None):
        self.validDate       = v or datetime.date.today()
        self.transactionDate = t or datetime.date.today()
        assert self.validDate <= self.transactionDate

class FredClock(_DBO):
    """Much like a standard clock, but the cutoffs are
       naked dates in the FRED datetime space.
       This may change only I understand the FRED data better."""
    
    @node
    def cosmicAll(self):
        return CosmicAll.get('TheCosmicAll', db=self.meta.db)
    
    @node(tweakable=True)
    def cutoffs(self):
        self.cosmicAll().dbState()
        return None

_tr.add(FredClock)
