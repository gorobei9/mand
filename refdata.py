
from obj import Entity, Event
from type_registry import _tr
from graph import node

class RefData(Entity):

    @node
    def clock(self):
        return _tr.Clock.get('RefData', db=self.meta.db)
    
    def state(self):
        evs = self.activeEvents()
        if evs:
            return evs[-1]

class RefDataUpdateEvent(Event):
    
    @node(stored=True)
    def entity(self):
        return None
    
    def str(self):
        return '%s: %-15s: %-15s: %-30s' % (self.meta._timestamp.validTime,
                                            self.fullName(), 
                                            self.address(), 
                                            self.company(),
                                            ) 
    
    
    @node
    def _containers(self):
        ret = set()
        ret.update(super(RefDataUpdateEvent, self)._containers())
        ret.add(self.entity())
        return ret

_tr.add(RefData)
_tr.add(RefDataUpdateEvent)
