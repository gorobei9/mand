
import datetime
from mand.core import _tr, node, Entity, Timestamp, Context, find, getNode
from mand.core import addFootnote
from mand.core import displayDict, displayMarkdown, displayListOfDicts, displayHeader
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
    
    @node(tweakable=True)
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

def makeWorld():
    def makeTree(names):
        ret = []
        for name in names:
            subs = [ TradingBook(name+str(i)) for i in range(10) ]
            p = TradingPortfolio(name).write()
            p.setChildren(subs)
            ret.append(p)
        return ret

    pAll = TradingPortfolio('TopOfTheHouse').write()
    subs = makeTree(['Eq-Prop', 'Eq-Inst', 'FX', 'Rates', 'Credit', 'Delta1', 'Loans', 'Commod', 'ETFs', 'Mtge'])
    pAll.setChildren(subs)

    bExt  = _tr.TradingBook('Customer1')
    bExt2 = _tr.TradingBook('Customer2')

    pWorld = TradingPortfolio('TheWorld').write()

    pWorld.setChildren([pAll, bExt, bExt2])

    print 'makeWorld, TopOfTheHouse is:', pAll
    print '    # books:', len(pAll.books())
    print '    # children:', len(pAll.children())
    
    return pWorld

def bookSomeTrades(pWorld):

    pAll, bExt, bExt2 = pWorld.children()

    with pAll.meta.db:

        s1_ibm  = MarketDataSource('source1.IBM')
        s1_goog = MarketDataSource('source1.GOOG')

        s1_ibm.update(last=175.61)
        s1_goog.update(last=852.12)

        ibm  = MarketInterface('IBM').write()
        goog = MarketInterface('GOOG').write()
        
        p1 = pAll.children()[0]
        p2 = pAll.children()[1]
        p3 = pAll.children()[2]
        p4 = pAll.children()[3]

        b1 = p1.children()[0]
        b2 = p2.children()[0]
        b3 = p3.children()[0]
        b4 = p4.children()[0]

        TradeOpenEvent = _tr.TradeOpenEvent
        cf1 = _tr.ForwardCashflow()
        ins1 = _tr.Equity()
        ins2 = _tr.Equity(assetName='GOOG.Eq.1')
        
        ts0 = Timestamp()

        for i in range(1000):
            ev0 = TradeOpenEvent(action='Buy',
                                 item=ins2,
                                 quantity=1,
                                 premium=cf1,
                                 unitPrice=852 + i/100.,
                                 book1=b3,
                                 book2=bExt2).write()
            
        for i in range(10):
            ev0 = TradeOpenEvent(action='Buy',
                                 item=ins2,
                                 quantity=1,
                                 premium=cf1,
                                 unitPrice=852 + i/100.,
                                 book1=b4,
                                 book2=bExt2).write()
                
                
        ts1 = Timestamp()
        
        ev1 = TradeOpenEvent(action='Buy',
                             item=ins1,
                             quantity=100,
                             premium=cf1,
                             unitPrice=175.65,
                             book1=b1,
                             book2=bExt).write()
        
        ts2 = Timestamp()
        
        s1_ibm.update(last=175.64)
        
        ts3 = Timestamp()
        
        ev2 = TradeOpenEvent(action='Buy',
                             item=ins2,
                             quantity=300,
                             premium=cf1,
                             unitPrice=852.12,
                             book1=b2,
                             book2=bExt).write()
        
        ev3 = TradeOpenEvent(action='Sell',
                             item=ins1,
                             quantity=100,
                             premium=cf1,
                             unitPrice=175.85,
                             book1=b2,
                             book2=bExt2).write()
        
        ts4 = Timestamp()
        
        s1_ibm.update(last=175.70)
        s1_goog.update(last=852.11)
        
        ts5 = Timestamp()
        
        s1_ibm.update(last=175.68)
        s1_goog.update(last=852.13)
        
        eod = Timestamp()
        
        ev4 = TradeOpenEvent(action='Buy',
                             item=ins1,
                             quantity=100,
                             premium=cf1,
                             unitPrice=175.69,
                             book1=b1,
                             book2=bExt,
                             amends=ev1,
                             message='Sorry, the broker says you actually paid 69. signed: the middle office'
                             ).write(validTime=ev1.meta._timestamp.validTime)
        
        s1_ibm.update(last=177.68)
        s1_goog.update(last=856.13)
        
        ts6 = Timestamp()
        
        return [ ts0, ts1, ts2, ts3, ts4, ts5, eod, ts6 ]


class PnLExplainReport(Entity):
    
    @node(stored=True)
    def valuable(self):
        return None
    
    @node(stored=True)
    def ts1(self):
        return None
    
    @node(stored=True)
    def ts2(self):
        return None

    @node
    def clocks(self):
        valuable = self.valuable()
        ts1 = self.ts1()
        ts2 = self.ts2()
        clock = valuable.getObj(_tr.RootClock, 'Main')
    
        def clks(ts):
            def fn(node):
                obj = node.key[0]
                m = node.key[1].split(':')[-1]
                if isinstance(obj, _tr.Clock) and m == 'cutoffs':
                    return True
            with Context({clock.cutoffs: ts}, 'Clocks'):
                nodes = find(valuable.NPV, fn)
                return dict( [ (node.tweakPoint, node) for node in nodes ] )
    
        allNodes = clks(ts1)
        allNodes.update(clks(ts2))
        return allNodes.values() 
        
    @node
    def data(self):
        valuable = self.valuable()
        ts1 = self.ts1()
        ts2 = self.ts2()
        clock = valuable.getObj(_tr.RootClock, 'Main')
    
        nodes = self.clocks()
    
        # IRL, we'd sort these according to some business req...
        # And our clocks might be arranged in an N-level tree...
        nodes = sorted(nodes, key = lambda node: node.key[0].meta.name())

        # NPV call counts:
        #  2 - clocks
        #  1 - start balance
        #  1 - start breaks
        #  3 - amend clocks
        #  3 - activity clocks
        #  1 - end breaks
        # --------------------
        # 11 TOTAL
        
        data = []
        curr = [0]
        def add(title, npv):
            pnl = npv - curr[0]
            curr[0] = npv
            data.append( {'Activity': title, 'PnL': pnl } )

        with Context({clock.cutoffs: ts1}, 'Start'):
            curr = [ valuable.NPV() ] # Starting balance
    
        tweaks = {}
        for n in nodes:
            tweaks[n.tweakPoint] = ts1
        with Context(tweaks, name='Start breaks'):
            start = valuable.NPV()
            add('Starting balance breaks', start)

        tsAmend = Timestamp(t=ts2.transactionTime, v=ts1.validTime)
        # XXX - modifying tweaks in place is a bit evil
        # This is only safe because I know Context() effectively copies, so this works
        # for now.
        for n in nodes:
            tweaks[n.tweakPoint] = tsAmend
            name = n.key[0].meta.name()
            with Context(tweaks, name='Amend %s' % name):
                add('prior day amends: %s' % name, valuable.NPV())
        for n in nodes:
            tweaks[n.tweakPoint] = ts2
            name = n.key[0].meta.name()
            with Context(tweaks, name='Activity %s' % name):
                add('activity: %s' % name, valuable.NPV())
    
        with Context({clock.cutoffs: ts2}, name='End'):
            end = valuable.NPV()
            add('Ending balance breaks', end)

        title = 'PnL explain for %s: %s' % (valuable.meta.name(), end-start)
        return data, title

    def run(self):
        data, title = self.data()
        node = getNode(self.data)
        footnotes = node.footnotes.values()
        displayHeader('%s' % title)
        if footnotes:
            displayMarkdown('**Caveat: this report encountered problems. See footnotes at bottom.**')
        displayListOfDicts(data, names=['Activity', 'PnL'] )
        if footnotes:
            displayMarkdown('## Footnotes:')
            txt = '\n'.join( [ '1. %s' % f for f in footnotes])
            displayMarkdown(txt)
            
