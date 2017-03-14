
from type_registry import _tr
from timestamp import Timestamp

class _ObjectDbBase(object):
    # A class more designed to avoid retyped code than doing anything good
    
    _dbs = [None]
    
    def __init__(self):
        self.cache = {}
        
    def __enter__(self, *a):
        self._dbs.append(self)
        
    def __exit__(self, *a):
        db = self._dbs.pop()
        assert db == self

    def get(self, name):
        if name not in self.cache:
            self.cache[name] = self._get(name)
        return self.cache[name]
    
    def _allEventNames(self, entity):
        return [ i['event'] for i in self._allEventRecords(entity) ]
    
    def _allEvents(self, entity):
        names = self._allEventNames(entity)
        objs = [ self.get(name) for name in names ]
        return objs
    
    def _wroteEvent(self):
        self.cosmicAll._wroteEvent()

    def describe(self):
        print self._describe()

    @classmethod
    def currentDb(cls):
        return _ObjectDbBase._dbs[-1]


class ObjectDb(_ObjectDbBase):
    # XXX - this class is still leaking abstraction from the DynamoDbDriver class.
    
    def __init__(self, foo=None, dbDriver=None, name=None, inMem=True, ro=None):
        # I actually learned this idiom from Google's Tensorflow code. Thanks, guys!
        # If you switch from a boa constructor to a keyword constructor, stick a
        # dummy named parameter at the front: calls using the old signature can be failed early.
        assert foo is None
            
        if dbDriver is None:
            from dbdriver import DynamoDbDriver
            dbDriver = DynamoDbDriver(name=name, inMem=inMem, ro=ro)
        self.dbDriver = dbDriver
        super(ObjectDb, self).__init__()
        self.name = 'O' + self.dbDriver.name

        _tr.RootClock('Main', db=self).write()
        self.cosmicAll = _tr.CosmicAll('TheCosmicAll', db=self).write()

    def copy(self):
        return ObjectDb(dbDriver=self.dbDriver)
    
    def _reify(self, d, path, db):
        if 'Item' not in d:
            return None
        item = d['Item']
        cls = _tr.cls(item['type'])
        obj = cls(None, db=db)
        ts = item['timestamp']
        if ts:
            ts = Timestamp.fromReadForm(ts)
        obj.meta._fromStoredForm(path, 
                                 _payload   = item['payload'], 
                                 _encoding  = item['encoding'], 
                                 _timestamp = ts)
        return obj
    
    def _get(self, name, db=None):
        if db is None:
            db = self
        return self._reify(self.dbDriver.getEntity(name), name, db)
    
    def put(self, item):
        self.dbDriver.putEntity(item)
        
    def _allEventRecords(self, entity):
        response = self.dbDriver.getMapEntries(entity)
        return response['Items']
        
    def _putMapItem(self, item):
        self.dbDriver.putMapEntry(item)
        
    def _describe(self):
        return '%s: %s' % (self, self.dbDriver._describe())

class UnionDb(_ObjectDbBase):
    def __init__(self, frontDb, backDb):
        self.frontDb = frontDb
        self.backDb = backDb
        super(UnionDb, self).__init__()
        self.name = '(%s:%s)' %  (self.frontDb.name, self.backDb.name)
        self.cosmicAll = _tr.CosmicAll('TheCosmicAll', db=self).write()
        
    def _get(self, name, db=None):
        # XXX - Really not adequate, would like caches on the child dbs,
        # then we cache a copy with db fixed up
        if db is None:
            db = self
        if name not in self.cache:
            o = self.frontDb._get(name, db=db)
            if o is not None:
                return o
            return self.backDb._get(name, db=db)

    def put(self, item):
        self.frontDb.put(item)
        
    def _allEventRecords(self, entity):
        af = self.frontDb._allEventRecords(entity)
        ab = self.backDb._allEventRecords(entity)
        # XXX - want to merge here, not sort. But these will be iterators soon, so don't bother:
        return sorted(af + ab, key=lambda r: r['key'])
        
    def _putMapItem(self, item):
        self.frontDb._putMapItem(item)
        
    def _describe(self):
        return '(%s %s)' % (self.frontDb._describe(), self.backDb._describe())
  
