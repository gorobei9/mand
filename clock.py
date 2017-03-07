
from obj import Entity, Event
from graph import node
from type_registry import _tr
from root_clock import RootClock

class Clock(Entity):
    @node
    def parent(self):
        evs = self.activeEvents()
        if evs:
            return evs[-1].parent()
        return RootClock.get('Main', db=self.meta.db)
    
    @node 
    def cutoffs(self):
        return self.parent().cutoffs()
    
    def setParent(self, parent):
        ev = ClockEvent(clock=self, parent=parent, db=self.meta.db)
        ev.write()
        
    def str(self):
        paths = []
        o = self
        while o:
            paths.append(o.meta.path())
            o = o.parent()
        return '<Clock: %s>' % ':'.join(paths)
            
class ClockEvent(Event):
    @node(stored=True)
    def clock(self):
        return None
    
    @node(stored=True)
    def parent(self):
        return None
    
    @node
    def _containers(self):
        return [self.clock()]

_tr.add(Clock)
_tr.add(ClockEvent)
