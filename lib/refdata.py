
from mand.core import Entity, Event, _tr, node

class RefDataUpdateEvent(Event):
    
    @node(stored=True)
    def entity(self):
        return None

    @node(stored=True)
    def data(self):
        return {}
    
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

class RefData(Entity):

    evCls = RefDataUpdateEvent
    
    @node
    def clock(self):
        return _tr.Clock.get('RefData', db=self.meta.db)

    def update(self, validTime=None, amends=[], **kwargs):
        ev = self.evCls(entity=self, amends=amends, data=kwargs, db=self.meta.db)
        ev.write(validTime=validTime)
        return ev

    @node
    def state(self):
        ret = {}
        for e in self.activeEvents():
            ret.update(e.data())
        return ret


_tr.add(RefData)
_tr.add(RefDataUpdateEvent)
