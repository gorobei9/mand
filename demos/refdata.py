
from ..type_registry import _tr
from ..graph import node
from ..timestamp import Timestamp
from ..context import Context
from ..db import ObjectDb, UnionDb
from ..dbdriver import DynamoDbDriver

class CustomerRefData(_tr.RefData):
    
    def fullName(self):
        return self.state().fullName()
       
    def address(self):
        return self.state().address()
    
    def company(self):
        return self.state().company()
    
    def comment(self):
        return self.state().comment()
    
    def update(self, validTime=None, **kwargs):
        ev = CustomerRefDataUpdateEvent(entity=self, **kwargs)
        ev.write(validTime=validTime)
        return ev
    
class CustomerRefDataUpdateEvent(_tr.RefDataUpdateEvent):
    
    @node(stored=True)
    def fullName(self):
        return None
    
    @node(stored=True)
    def address(self):
        return None
    
    @node(stored=True)
    def company(self):
        return None
    
    @node(stored=True)
    def comment(self):
        return None
    
    @node
    def _validTime(self):
        return self.meta._timestamp._str()
    
    @node
    def _entityName(self):
        return self.entity().meta.name()
        
    def _uiFields(self, key=None):
        return [ '_validTime', 'fullName', 'address', 'company', '_entityName' ]
    
    def str(self):
        return '%s: %-15s: %-15s: %-30s: %s' % (self.meta._timestamp.validTime,
                                                self.fullName(), 
                                                self.address(), 
                                                self.company(),
                                                self.entity().meta.name(),
                                                ) 

    
_tr.add(CustomerRefData)
_tr.add(CustomerRefDataUpdateEvent)


def main(rawdb, _odb):
    
    with _odb:
        clock = _tr.Clock('RefData').write()

        cr = CustomerRefData('Customer123')

        cr.update(fullName='Eliza Smith',
                  address='10 Main St',
                  company='Frobozz Magic Avocado Company')

        ts1 = Timestamp()

        e2 = cr.update(fullName='Eliza Smith',
                       address='10 Main St',
                       company='Frobozz Magic Friut Company')

        ts2 = Timestamp()

        cr.update(fullName='Eliza Smith',
                  address='10 Main St',
                  company='Frobozz Magic Fruit Company',
                  comment='Grr. Typo.',
                  amends = e2)

        ts3 = Timestamp()

        e4 = cr.update(fullName='Eliza James',
                       address='10 Main St',
                       company='Frobozz Magic Fruit Company')



    cr.printActivity()


    def info(customer, verbose=True):
        if verbose:
            print 'Clock is at:', customer.clock().cutoffs()
        print 'data: %s works for %s, located at %s' % (customer.fullName(), customer.company() , customer.address())
        if verbose:
            print
            customer.printActivity(customer._visibleEvents)
            customer.printActivity(customer.activeEvents)
            print
            print
                
    info(cr)
    with Context({clock.cutoffs: ts1}) as ctx:
        info(cr)
                    


    # ## Testing that a different client sees the same data [Test]


    print 'original db  :',  _odb._describe()

    _db2 = ObjectDb(rawdb)
    print 'new client db:',  _db2._describe()
    print

    c = _db2.get(cr.meta.path())
    assert c.clock() is not clock
    print 'current:', c.fullName()
    assert c.fullName() == 'Eliza James'

    with Context({c.clock().cutoffs: ts1}) as ctx:
        print 'at ts1 :', c.fullName()
        assert c.fullName() == 'Eliza Smith'
        print
    assert not c.meta.isNew


    # ## Testing modification in a private copy of the db [Test]

    print 'Prod db:', _odb._describe()
    print

    _dbNew = ObjectDb(DynamoDbDriver(rawdb._ddb))

    _dbU = UnionDb(_dbNew, _odb)

    customer = _dbU.get(cr.meta.path())

    print 'in production:'
    info(customer, verbose=False)

    ev4 = customer.activeEvents()[-1]
    print 'deleting union event:', ev4, ev4.meta.db.name
    ev4.delete()

    print
    print 'in union:'

    info(customer, verbose=False)

    print 'in production:'
    _dbProd = ObjectDb(rawdb)

    customer = _dbProd.get(cr.meta.path())
    info(customer, verbose=False)

    print 'Prod db:', _odb._describe()
    print 'New db:', _dbNew._describe()
    print 'Union db:', _dbU._describe()


    # ## Testing valid times (i.e. back-dated transactions) [Test]

    with _odb:

        cr = CustomerRefData('Customer.joe')

        clock = cr.clock()

        cr.update(fullName='Joe James',
                  address='235 W 76',
                  company='Frobozz Magic Lamp Company')

        endOfDay = Timestamp()

        print 'Line item end of day:'
        with Context({clock.cutoffs: endOfDay}):
            info(cr, verbose=False)

        e2 = cr.update(fullName='Joe James',
                       address='1 First Ave',
                       company='Frobozz Magic Lamp Company',
                       validTime=endOfDay.validTime,
                       comment = 'Oops, we got this address change yesterday'
                       )

        e3 = cr.update(fullName='Joe James',
                       address='235 W 76',
                       company='Frobozz Magic Illumination Company',
                       )

        ts3 = Timestamp()

        endOfDayCorrected = Timestamp(v=endOfDay.validTime)

    print 'Line item end of day (rerun):'
    clock = cr.clock()
    with Context({clock.cutoffs: endOfDay}):
        info(cr, verbose=False)
    print

    print 'Line item generated from corrected end of day report:'
    with Context({clock.cutoffs: endOfDayCorrected}):
        info(cr, verbose=False)
    print

    print 'Current line item:'
    info(cr, verbose=False)
