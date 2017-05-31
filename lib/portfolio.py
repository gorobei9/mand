
from mand.core import node, _tr, merge
from mand.core import addFootnote
from mand.lib.refdata import RefDataUpdateEvent, RefData
            
class PortfolioUpdateEvent(RefDataUpdateEvent):

    @node(stored=True)
    def children(self):
        return []
    
class Portfolio(RefData):

    evCls = PortfolioUpdateEvent
    
    @node
    def clock(self):
        return self.getObj(_tr.Clock, 'Portfolio')
    
    @node
    def children(self):
        evs = self.activeEvents()
        if evs:
            return evs[-1].children()
        else:
            return []
        
    def setChildren(self, children, validTime=None, amends=[]):
        ev = self.evCls(entity=self, amends=amends, children=children, db=self.meta.db)
        ev.write(validTime=validTime)
        return ev
    
    @node
    def books(self):
        books = set()
        for c in self.children():
            for b in c.books():
                if b in books:
                    addFootnote(text='Book appears multiple times', info=b.meta.path())
                books.add(b)
        return list(books)

    @node
    def items(self):
        ret = {}
        for c in self.children():
            merge(ret, c.items())
        self.books() # force a consistency check.
        return ret

    @node
    def tickets(self):
        ret = set()
        for c in self.children():
            ret.update(c.tickets())
        self.books() # force a consistency check.
        return list(ret)
    
    def prn(self, depth=0):
        print('  '*depth, self.meta.name())
        for c in self.children():
            c.prn(depth+1)
            
_tr.add(Portfolio)
_tr.add(PortfolioUpdateEvent)
