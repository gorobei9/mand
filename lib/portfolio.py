
from mand.core import node, _tr
from mand.lib.refdate import RefDataUpdateEvent, RefData
            
class PortfolioUpdateEvent(RefDataUpdateEvent):

    @node(stored=True)
    def children(self):
        return []
    
class Portfolio(RefData):

    evCls = PortfolioUpdateEvent
    
    @node
    def clock(self):
        return _tr.Clock.get('Portfolio', db=self.meta.db)
    
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
                    print 'LogMessage: Oops, book appears multiple times'
                books.add(b)
        return list(books)

    def prn(self, depth=0):
        print '  '*depth, self.meta.name()
        for c in self.children():
            c.prn(depth+1)
            
_tr.add(Book)
_tr.add(Portfolio)
_tr.add(PortfolioUpdateEvent)
