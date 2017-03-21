
from mand.core import _tr

def setUpDb(db,
          isTrading=True):

    _tr.CosmicAll.get('TheCosmicAll', db=db, create=True)
    _tr.RootClock.get('Main', db=db, create=True)

    if isTrading:
        rd = _tr.Clock.get('RefData', db=db, create=True)
        wf = _tr.Clock.get('Workflow', db=db, create=True)
        
        _tr.Clock.get('Portfolio', db=db, create=True)
        
        c = _tr.Clock.get('Trading', db=db, create=True)
        if not c.activeEvents():
            c.setParent(wf)

        c = _tr.Clock.get('MarketData', db=db, create=True)
        if not c.activeEvents():
            c.setParent(rd)
            

    
