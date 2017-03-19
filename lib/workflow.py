
from mand.core import _tr, Entity, Event, node, Clock, merge, flatten, strForm

class Workbook(Entity):
    
    @node
    def clock(self):
        return self.getObj(_tr.Clock, 'Workflow')
    
    def tickets(self):
        # A book has a ticket iff the ticket is contributing an item to the book
        evs = self.activeEvents()
        ret = {}
        for e in evs:
            ibb = e._itemsByBookByTicket() # { book: { ticket: { item: q }}}
            items = ibb.get(self, {})      # { ticket: (item: q } )}
            merge(ret, items)
        return ret.keys()
  
    @node
    def refData(self):
        return _tr.CustomerRefData.get(self.meta.name(), db=self.meta.db)

    # the following are mostly for use by the Portfolio class...
    
    def books(self):
        return [self]

    def prn(self, depth=0):
        print '  '*depth, self.meta.name()

class WorkTicket(Entity):
    
    @node
    def clock(self):
        return Clock.get('Workflow', db=self.meta.db)
    
    @node
    def state(self):
        evs = self.activeEvents()
        ret = {}
        for e in evs:
            bbi = e._itemsByTicketByBook() # { ticket: { book: { item: q }}}
            books = bbi.get(self, {})      # { book: { item: q }}
            merge(ret, books)
        return ret
    
    def _itemsByBook(self):
        """returns { book: { item: q }}"""
        return self.state()
    
    def _item(self):
        items = flatten(self._itemsByBook(), deleteZeros=False)
        if len(items) == 1:
            return items.keys()[0]
        
    def sourceBook(self):
        b = [ k for k, d in self._itemsByBook().items() if any( [ v > 0 for v in d.values() ] ) ]
        if len(b) == 1:
            return b[0]
        
    def destinationBook(self):
        b = [ k for k, d in self._itemsByBook().items() if any( [ v < 0 for v in d.values() ] ) ]
        if len(b) == 1:
            return b[0]
        
    @node
    def openEvent(self):
        evs = self.activeEvents()
        op = [ e for e in evs if isinstance(e, WorkItemOpenEvent) ]
        if len(op) > 1:
            assert False
        return op[0] if op else None

    @node
    def _containers(self):
        ret = [ b for b in [ self.sourceBook(), self.destinationBook() ] if b ]
        return ret
    
    def str(self, source=None):
        if source == self.sourceBook():
            o = self.destinationBook()
            name = o.meta.name() if o else '?'
            extra = ', waiting on %s' % name
        elif source == self.destinationBook():
            o = self.sourceBook()
            name = o.meta.name() if o else '?'
            extra = ', raised by %s' % name
        else:
            extra = ''
        return 'Ticket %s, %s (%s events, %s active)' % (self.meta.name(), 
                                                         extra,
                                                         len(self._visibleEvents()), 
                                                         len(self.activeEvents()))
    
    def addMessage(self, message):
        ev = WorkItemMessageEvent(message=message, ticket=self, db=self.meta.db)
        ev.write()
        return ev
        
    def transfer(self, book1, book2, action='transfer'):
        item = self._item()
        quantity = -self._itemsByBook()[book1][item] # remove all of item from first book
        ev = WorkItemTransferEvent(ticket=self, 
                                   item=item,
                                   quantity=quantity,
                                   book1=book1,
                                   book2=book2, 
                                   action=action, 
                                   db=self.meta.db)
        ev.write()
        return ev


class WorkItem(Entity):
    pass


# Event types:
#  Open
#  Close?
#  Aggregate  - ticket* -> new ticket
#  Split      - ticket -> ticket*
#  Transfer/Novation
#  MutateItem

class _WorkItemEvent(Event):
    
    @node(stored=True)
    def ticket(self):
        ret = WorkTicket(db=self.meta.db)
        return ret
    
    @node(stored=True)
    def item(self):
        ret = WorkItem(db=self.meta.db)
        return ret
    
    @node(stored=True)
    def book1(self):
        return None
    
    @node(stored=True)
    def book2(self):
        return None

    def _items(self):
        return [ [ self.ticket(), self.item(), 1, self.book1(), self.book2() ] ]
    
    def _itemsByBookByTicket(self):
        """returns { book: {ticket: {item: q }}}"""
        ret = {}
        for t, i, q, b1, b2 in self._items():
            merge(ret, { b1: { t: { i:  q }}} )
            merge(ret, { b2: { t: { i: -q }}} )
        return ret

    def _itemsByTicketByBook(self):
        """returns { ticket: {book: {item: q }}}"""
        ret = {}
        for t, i, q, b1, b2 in self._items():
            merge(ret, { t: { b1: { i:  q }}} )
            merge(ret, { t: { b2: { i: -q }}} )
        return ret
    
    @node
    def _containers(self):
        ret = set()
        ret.update(super(_WorkItemEvent, self)._containers())
        ret.add(self.book1())
        ret.add(self.book2())
        ret.add(self.ticket())
        return ret
    
    def _uiFields(self, key=None):
        return ['action', 'book1', 'book2']


class WorkItemOpenEvent(_WorkItemEvent):
    
    @node(stored=True)
    def message(self):
        return None
    
    def action(self):
        return 'open'
    
    def str(self, source=None):
        if self.book1() == source:
            extra = 'raised with: %s' % self.book2().meta.name()
        elif self.book2() == source:
            extra = 'raised by: %s' % self.book1().meta.name()
        else: 
            extra = '???'
        return 'New, %s: %s, msg: %s' % (self.meta.name(), extra, self.message())


class WorkItemTransferEvent(_WorkItemEvent):
    
    @node(stored=True)
    def action(self):
        return 'transfer'
    
    @node(stored=True)
    def quantity(self):
        return 1
    
    def _items(self):
        return [ [ self.ticket(), self.item(), self.quantity(), self.book1(), self.book2() ] ]
    
    def str(self, source=None):
        return 'Transfer, %s: %s -> %s' % (self.meta.name(), self.book1().meta.name(), self.book2().meta.name())


class WorkItemMessageEvent(Event):
    
    @node(stored=True)
    def ticket(self):
        return None
    
    @node(stored=True)
    def message(self):
        return None
    
    def _itemsByBookByTicket(self):
        return {}

    def _itemsByTicketByBook(self):
        return {}
 
    def action(self):
        return 'message'
    
    @node
    def _containers(self):
        ret = set()
        ret.update(super(WorkItemMessageEvent, self)._containers())
        ret.add(self.ticket())
        c = self.ticket()._containers()
        ret.update(c)
        return ret
    
    def str(self, source=None):
        m = self.message()
        return 'Message: %s' % strForm(m, 60)

    def _uiFields(self, key=None):
        return ['action', 'message']


_tr.add(Workbook)
_tr.add(WorkTicket)
_tr.add(WorkItem)
_tr.add(WorkItemOpenEvent)
_tr.add(WorkItemTransferEvent)
_tr.add(WorkItemMessageEvent)

