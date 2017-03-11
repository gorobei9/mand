

from objmeta import EntityMeta, EventMeta
from graph import node
from utils import displayTable
from type_registry import _tr
from dbo import _DBO
from root_clock import RootClock

class Entity(_DBO):
    _instanceMetaclass = EntityMeta
    
    def _allEvents(self):
        db = self.meta.db
        return db._allEvents(self)
    
    @node
    def clock(self):
        db = self.meta.db
        return RootClock.get('Main', db=db)
    
    def _visibleEvents(self):
        evs = self._allEvents()
        cutoffs = self.clock().cutoffs()
        if cutoffs:
            tTime = cutoffs.transactionTime
            evs = [ e for e in evs if e.meta._timestamp.transactionTime <= tTime ]
        return evs
    
    def activeEvents(self):
        evs = self._visibleEvents()
        cutoffs = self.clock().cutoffs()
        deletes = set()
        cancels = set()
        active = []
        for e in reversed(evs):
            if e in deletes:
                continue
            if isinstance(e, DeleteEvent):
                deletes.update(e._amends())
                continue
            if isinstance(e, CancelEvent):
                cancels.update(e._amends())
                continue
            else:
                deletes.update(e._amends())
            if e in cancels:
                continue
            active.append(e)

        if cutoffs:
            vTime = cutoffs.validTime
            active = [ e for e in active if e.meta._timestamp.validTime <= vTime ]
        return sorted(active, key=lambda e: e.meta._timestamp.validTime)
    
    def printActivity(self, evsFn=None):
        if evsFn is None:
            evsFn = self.activeEvents
        print '%s %s:' % (self, evsFn.func_name)
        evs = evsFn()
        displayTable(evs, )
       
    def str(self):
        return '<%s, isNew=%s>' % (self.meta.path(), self.meta.isNew)


class Event(_DBO):
    _instanceMetaclass = EventMeta

    @node(stored=True)
    def amends(self):
        return []
    
    def action(self):
        return 'unknown action'
    
    @node
    def _containers(self):
        ret = set()
        for a in self._amends():
            ret.update(a._containers())
        return ret
    
    def write(self, validTime=None):
        ret = super(Event, self).write(validTime=validTime, _containers=self._containers())
        self.meta.db._wroteEvent()
        return ret
        
    def _amends(self):
        a = self.amends()
        if isinstance(a, list):
            return a
        if a:
            return [a]
        return []
    
    def str(self):
        return '<%s, isNew=%s, ts=%s>' % (self.meta.path(), self.meta.isNew, self.meta._timestamp)
    
    def _uiFields(self, key=None):
        return [ 'action' ]
    
    def delete(self):
        e = DeleteEvent(amends=self, db=self.meta.db)
        return e.write()
    
    def cancel(self):
        e = CancelEvent(amends=self, db=self.meta.db)
        return e.write()
    
class DeleteEvent(Event):
    def action(self):
        return 'delete'

class CancelEvent(Event):
    def action(self):
        return 'cancel'

_tr.add(DeleteEvent)
_tr.add(CancelEvent)
