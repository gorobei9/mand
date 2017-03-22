
import datetime
from mand.core import _tr, node, Entity
from mand.core import addFootnote
from mand.lib.extrefdata import ExternalRefData, dataField
from mand.lib.workflow import Workbook, WorkItemOpenEvent, WorkItem
from mand.lib.portfolio import Portfolio

class TradingContainer(Entity):

    @node
    def NPV(self):
        ret = 0
        for i, q in self.items().items():
            ret += i.NPV() * q
        return ret

class TradingBook(TradingContainer, Workbook):
    @node
    def clock(self):
        return self.getObj(_tr.Clock, 'Trading')

class TradingPortfolio(TradingContainer, Portfolio):
    pass

class TradingTicket(TradingContainer):
    pass
    
_tr.add(TradingBook)
_tr.add(TradingPortfolio)
_tr.add(TradingTicket)

class MarketDataSource(ExternalRefData):
    @node
    def clock(self):
        return self.getObj(_tr.Clock, 'MarketData')

    @dataField
    def last(self):
        return None
    
_tr.add(MarketDataSource)

class MarketInterface(Entity):
    
    @node
    def sourceName(self):
        return 'source1'
    
    @node
    def source(self):
        return self.getObj(_tr.MarketDataSource, '%s.%s' % (self.sourceName(), self.meta.name()))
    
    @node
    def spot(self):
        return self.source().last()
                           
    
_tr.add(MarketInterface)

class Instrument(WorkItem):
    """A thing that can be owned, an asset, or legal obligation"""
    
class ForwardCashflow(Instrument):
    
    @node(stored=True)
    def currency(self):
        return 'USD'
    
    @node(stored=True)
    def settlementDatetime(self):
        d = datetime.datetime.utcnow() + datetime.timedelta(2)
        return d
    
    @node
    def NPV(self):
        # XXX - would really get the currency discount curve here, and discount according to 
        # current time/settlement time
        # XXX - and do a conversion to our native currency
        addFootnote(text='Inadequate cash discounting model used')
        return 1
    
    @node
    def name(self):
        return 'Cash %s/%s' % (self.currency(), self.settlementDatetime())


class Equity(Instrument):
    
    @node(stored=True)
    def assetName(self):
        return 'IBM.Eq.1'
    
    @node
    def NPV(self):
        return self.refdata().spot()
    
    @node
    def refdata(self):
        return self.getObj(_tr.MarketInterface, self.assetName().split('.')[0])

    @node
    def name(self):
        return 'Stock: %s' % self.assetName()
    
_tr.add(ForwardCashflow)
_tr.add(Equity)

class TradeOpenEvent(WorkItemOpenEvent):
    @node(stored=True)
    def ticket(self):
        ret = TradingTicket(db=self.meta.db)
        return ret

    @node(stored=True)
    def action(self):
        return 'Buy'
    @node(stored=True)
    def quantity(self):
        return 1.
    @node(stored=True)
    def premium(self):
        return None
    @node(stored=True)
    def unitPrice(self):
        return 0.
    
    def _items(self):
        bs = 1 if self.action() == 'Buy' else -1
        pq = -bs * self.unitPrice() * self.quantity()
        return [ [ self.ticket(), self.item(),    bs*self.quantity(), self.book1(), self.book2() ],
                 [ self.ticket(), self.premium(), pq,                 self.book1(), self.book2() ]
               ] 

_tr.add(TradeOpenEvent)
