
# coding: utf-8

# # AbstractNonsenseDb: A bitemporal, no-SQL, object-oriented, compute-aware, cloud-friendly database
# 
# Note that this is experimental code. It's not even a POC: 
# * The persistence layer is in-memory
# * It has little error detection
# * It will fail as soon as the dataset gets bigger than trivial
# * Most important components are just stub/mock implementations
# * There is no attempt to be efficient in the small
# 
# That said:
#     
# * The design should scale to large datasets and large compute farms
# * It's pretty back-end database agnostic: I'm using Amazon DynamoDb, but Google Datastore, etc, should work fine
# * It will support several other features (data lineage, footnoting)
# * It should explain the basic concepts needed to understand version 2 (the POC)
# 
# ## Features & Limitations
# 
# ### Bitemporal
# 
#   * It's temporal: you can query the database as it was at any previous point in time
#   * It has a notion of back-dated amends: you can query as of time t given your knowledge at time t+n
#   * There can be multiple time-lines, with entities on their own time-lines. 
#     So most Dr. Who episodes should be expressable
#   * Time is implicit: the same query or report that runs on the current db state can run historically 
#     without modification
#   * But, there is no algebra of time, so don't hope for native queries such as 
#     "during what time periods was Elizabeth married to Richard?"
#     
# ### No-SQL
#   
# This is a key-value system. The name of a thing is enough to find it, so no need to know about tables or data 
# schemas, etc. But, don't expect joins, projects, etc.
# 
# ### Object-oriented
# 
# * Database entities are exposed to a client process as standard objects within the client language
# * Objects may reference other objects (aka "point to") 
# * Objects have attributes that may be either stored or calculated from other methods
# 
# ### Compute-aware
# 
# The system may return cached results for, or remotely compute, certain object.method calls.
# 
# ### Cloud-friendly
# 
# A design goal is to lever cloud services such as elastic storage and compute without adding a layer of 
# abstraction or tooling.
# 
# 
# ## Notes
# 
# This notebook is quite large. To help keeping you oriented, some sections may be marked as follows:
#     
# 1. **[System]**. Code that actually does something concrete. E.g. manages a resource
# 2. **[Core]**. Code that makes the overall system hang together, even if it initially seems pointless or just a 
# junk abstraction
# 3. **[DBA]**. Code that defines generic schemas and access patterns. Author would be similar to a traditional database
# administrator managing table layouts and indexes
# 4. **[BA]**. Code that actually does something to solve a business problem
# 5. **[User]**. Code that a user might write or use
# 6. **[Test]**. Code to test things are behaving as expected
# 
# If I had staff, I'd split people up into these roles.

# In[145]:

import boto3
import pprint
from boto3.dynamodb.conditions import Key
import datetime
import dateutil

printWrites = False


# In[146]:

# Assumes external process:
#   java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -sharedDb --inMemory

ddb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')
#dc = boto3.client('dynamodb', endpoint_url='http://localhost:8000')


# ## Interface to a physical db [System] 
# 
# The only exposed methods are reading and writing entities and map items. 
# 
# CRUD (create-read-update-delete)? This is a CR driver: the UD elements will be implemented higher up in the stack.
# That's how we will implement the whole bi-temporality thing.
# 
# For this workbook, we just use in-memory tables with incrementing names so that we can get an empty db with each
# instance of a db driver...

# In[147]:

try:
    _dbn
except:
    _dbn = 0
_dbn += 1

class DynamoDbDriver(object):
    def __init__(self, ddb):
        self._ddb = ddb
        
        global _dbn
        
        self.name = 'DDB-MEM-%s' % _dbn
        
        entityTableName = 'entities_%s' % _dbn
        mapTableName    = 'map_%s' % _dbn
        
        _dbn += 1
        
        self._entities = ddb.create_table(
            TableName             = entityTableName,
            KeySchema             = [ { 'AttributeName': 'name', 'KeyType': 'HASH' }, ],
            AttributeDefinitions  = [ { 'AttributeName': 'name', 'AttributeType': 'S' }, ],
            ProvisionedThroughput = { 'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5 }
            )
        self._entities.meta.client.get_waiter('table_exists').wait(TableName=entityTableName)

        self._map = ddb.create_table(
            TableName             = mapTableName,
            KeySchema             = [ {'AttributeName': 'entity', 'KeyType': 'HASH'},
                                      {'AttributeName': 'key',    'KeyType': 'RANGE'}],
            AttributeDefinitions  = [ {'AttributeName': 'entity', 'AttributeType': 'S'},
                                      {'AttributeName': 'key',    'AttributeType': 'S'},],
            ProvisionedThroughput = {'ReadCapacityUnits': 5,'WriteCapacityUnits': 5}
            )
        self._map.meta.client.get_waiter('table_exists').wait(TableName=mapTableName)
     
    def getEntity(self, name):
        return self._entities.get_item(Key={'name': name})
    def putEntity(self, item):
        self._entities.put_item(Item=item)
        
    def getMapEntries(self, entity):
        return self._map.query(KeyConditionExpression=Key('entity').eq(entity.meta.path()))
    def putMapEntry(self, item):
        self._map.put_item(Item=item)
        
    def _describe(self):
        return '%s: entities=%s, map=%s' % (self.name, 
                                            self._entities.item_count, 
                                            self._map.item_count)


# ## Getting the class from a database object [Core]
# 
# Database items (rows) are just dictionaries of keys and simple values such as strings, numbers, and lists.
# 
# Our database items will need to say what class the item corresponds to, and we need to able to get the code for that
# class to convert the item (row) to a Python object or similar. 
# 
# A real implementation would register classes by name/module, and load class definition scripts as needed. 

# In[148]:

class TypeRegistry(object):
    def __init__(self):
        self.clsToName = {}
        self.nameToCls = {}
    def add(self, cls):
        """XXX - Testing method - just register an in-process class with its own name"""
        name = cls.__name__
        self.clsToName[cls] = name
        self.nameToCls[name] = cls
    def cls(self, name):
        return self.nameToCls[name]
    def name(self, cls):
        return self.clsToName[cls]
    
_tr = TypeRegistry()


# ## Naming anonymous objects [Core]
# 
# Many objects in our db won't have a useful name. Some entities might have a useful name (e.g. "/Countries/Germany",)
# but many will not (e.g. all events we record.)
# 
# A real implementation would call out to some global service.

# In[149]:

_uid = 10000
def getUUID():
    global _uid
    _uid = _uid+1
    s = str(_uid)
    return '%02d.%s' % (len(s), s)


# ## When do things happen? [Core]
# 
# The database will consist of:
# * entities (named things that exist for eternity)
#   * entities have no stored values
#   * entities may have state that is constructed from events that affect them
# * events (things we observe that may change the state of entities) 
#   
# Timestamps describe when an event happened. 
# * transactionTime is when the event hit the database
# * validTime is when the event should have been observed
# 
# The definition of validTime is a bit messy, but thinking of it as 'legal' or 'business' time might be helpful.
# 
# For example: 
# 1. you get a utilities bill in the mail on Monday. 
# 1. you have no money, so you avoid opening the letter until Wednesday.
# 1. on Friday, you record the bill in your banking app.
# 
# The transaction time is Friday, the valid time is Monday.
# Wednesday is the observation time, but everyone except you only cares about Monday.
# 
# Note this implies valid times cannot ever be greater than transaction times for an event.
# An event might reference a future time (e.g. bill due on Jan 31, 2017,) but that is payload data, not the valid
# time.

# In[150]:

class Timestamp(object):
    def __init__(self, t=None, v=None):
        self.validTime       = v or datetime.datetime.utcnow()
        self.transactionTime = t or datetime.datetime.utcnow()
        assert self.validTime <= self.transactionTime
    def indexStr(self):
        return self.transactionTime.isoformat()
    def _str(self):
        return self.validTime.isoformat()
    def writeForm(self):
        return [ self.transactionTime.isoformat(), self.validTime.isoformat() ]
    def __repr__(self):
        return '<TS:t=%s,v=%s>' % (self.transactionTime.isoformat(), self.validTime.isoformat())
    
    @classmethod
    def fromReadForm(cls, v):
        return Timestamp(dateutil.parser.parse(v[0]), 
                         dateutil.parser.parse(v[1]))


# ## The user-level db interface [Core]
# 
# This is largely a helper class for python objects to serialize/deserialize themselves and their references.
# 
# * supports put()/get() of entities
# * supports association of map records with entities
# * supports *with db: ...* syntactic sugar so we can add objects to a database without too much noise 
#     (ugh: user level stuff)

# In[151]:

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
    
    def describe(self):
        print self._describe()
        
def currentDb():
    return _ObjectDbBase._dbs[-1]


# In[152]:

class ObjectDb(_ObjectDbBase):
    # XXX - this class is still leaking abstraction from the DynamoDbDriver class.
    
    def __init__(self, dbDriver):
        self.dbDriver = dbDriver
        super(ObjectDb, self).__init__()
        self.name = 'O' + self.dbDriver.name
        
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
  
rawdb = DynamoDbDriver(ddb)
_odb = ObjectDb(rawdb)


# ## Oh, the Places You'll Go!
# 
# So, let us say we want to connect to a production database and experiment with adding/deleting stuff, etc.
# The old school method is to make a snapshot and work on that: no harm to the prod db, but maybe a lot of data
# copying to get the test environment ready.
# 
# Because our CRUD semantics will be implemented on top of CR semantics, we can just join an empty db in front
# of our production db. If all writes are directed to the front db, we can't damage production, but still
# have a first-class modifiable, live copy.
# 
# There are a lot more things we can do with unions than that. But here's a basic implementation:

# In[153]:

class UnionDb(_ObjectDbBase):
    def __init__(self, frontDb, backDb):
        self.frontDb = frontDb
        self.backDb = backDb
        super(UnionDb, self).__init__()
        self.name = '(%s:%s)' %  (self.frontDb.name, self.backDb.name)
        
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
  


# ## From in-memory values to database values [Core]
# 
# In-memory values might not be storable in our physical db.
# The EncDec class provides the mapping from values in our in-memory objects to database items.
# 
# For now, only support references to other objects, and only if they are trivial.

# In[154]:

class EncDec(object):
    # XXX - encoding really depends on the underlying physical db, this class
    # should be more tied to that
    decoders = {}
    encoders = []
    
    def __init__(self, name, test, encode, decode):
        self.name = name
        self.test = test
        self.encode = encode
        self.decode = decode
        assert name not in self.decoders
        self.encoders.append(self)
        self.decoders[name] = decode
        
    @classmethod
    def encode(cls, value):
        for e in cls.encoders:
            if e.test(value):
                return e.name, e.encode(value)
        return None, value
    
    @classmethod
    def decode(cls, name, value, meta):
        if name is None:
            return value
        return cls.decoders[name](value, meta)
    
def encode(v):
    return EncDec.encode(v)

def decode(t, v, meta):
    return EncDec.decode(t, v, meta)
    
def addEncoding(name, test, encode, decode):
    EncDec(name, test, encode, decode)

def _persist(o):
    o.write()
    return o

addEncoding('O',
            lambda v: isinstance(v, _DBO),
            lambda v: _persist(v).meta.path(),
            lambda v, meta: meta.db.get(v)
           )
                   
addEncoding('OL',
            lambda v: isinstance(v, list) and v and isinstance(v[0], _DBO),
            lambda v: [ _persist(o).meta.path() for o in v ],
            lambda v, meta: [ meta.db.get(p) for p in v ]
           )         


# # That's it for the database code!
# 
# Now we can work on getting Python objects into and out of the database...

# In[155]:

class NoVal(object):
    """trivial singleton: our version of None because user code will use None"""
    pass

_noVal = NoVal()


# ## Context - Dynamic bindings we use as we compute values [Core]
# 
# This is basically the environment object in *SICP*'s eval-apply: a set of bindings of values to names.
# Names are just entity methods.
# 
# Ignore this class and the class's internal cache for now. Sorry.

# In[156]:

class Context(object):
    _contexts = []
    
    def __init__(self, tweaks):
        p = self.parent()
        self.tweaks = p.tweaks.copy() if p else {}
        self.tweaks.update(tweaks)
        self.cache = self.tweaks.copy()
    def __enter__(self, *a):
        self._contexts.append(self)
    def __exit__(self, *a):
        c = self._contexts.pop()
        assert c == self
    def get(self, cmb):
        return self.cache.get(cmb, _noVal)
    def set(self, cmb, v):
        self.cache[cmb] = v
  
    @classmethod
    def current(cls):
        if not cls._contexts:
            cls._contexts.append(Context({}))
        return cls._contexts[-1]
    
    @classmethod
    def parent(cls):
        if len(cls._contexts) > 1:
            return cls._contexts[-2]
    
    @classmethod
    def inRootContext(cls):
        return cls.current() == cls._contexts[0]
    


# ## Object meta [Core]
# 
# All db-aware objects in the client may have come from or will go to the database.
# The DBOMeta metaclass handles the mechanics. This is mostly to avoid polluting the namespace of
# the user objects.
# 
# The core elements are:
# * _data: the values of the underlying object 
# * _payload: the values we actually persist to the database
# 
# Note we keep everything lazy on read: we don't reify a field value (call the decode method) until it is asked for.

# In[157]:

class DBOMeta(object):
    def __init__(self, obj, name=None, db=None, kwargs=None):
        if db is None:
            db = currentDb()
        self.typeId       = _tr.name(obj.__class__)
        self._name        = name
        self.db           = db
        self._payload     = None
        self._encoding    = None
        self._timestamp   = None
        self._data        = kwargs
        self.isNew        = True
    def _fromStoredForm(self, path, _payload, _encoding, _timestamp):
        prefix = self._prefix()
        assert path.startswith(prefix)
        self._name = path[len(prefix):]
        op = {}
        while _encoding:
            v = _encoding.pop()
            k = _encoding.pop()
            op[k] = v
        self._payload   = _payload
        self._encoding  = op
        self._timestamp = _timestamp
        self.isNew      = False
    def _prefix(self):
        return '/Global/%s/' % self.typeId
    def name(self):
        if self._name is None:
            self._name = getUUID()
        return self._name
    def path(self):
        return '%s%s' % (self._prefix(), self.name())
    def _toStoredForm(self):
        if self._encoding is None:
            enc = {}
            payload = {}
            for k, v in self._data.items():
                t, s = encode(v)
                payload[k] = s
                if t:
                    enc[k] = t
            self._payload = payload
            self._encoding = enc
            
    def getField(self, name):
        if self._payload and name in self._payload:
            p = self._payload[name]
            t = self._encoding.get(name)
            v = decode(t, p, self)
            return v
        elif self._data and name in self._data:
            return self._data[name]
        return _noVal
    
    def setField(self, name, value):
        assert self.isNew
        assert name not in self._data
        self._data[name] = value
        
    def _write(self, timestamp):
        if not Context.inRootContext():
            raise RuntimeError('non root-context write semantics not yet figured out.')
        path = self.path()
        self._toStoredForm() 
        if printWrites:
            print 'Writing (meta)', path, self.db.name
        op = []
        for k, v in self._encoding.items():
            op.append(k)
            op.append(v)
        item = {'name':      path,
                'type':      self.typeId,
                'payload':   self._payload,
                'encoding':  op,
                'timestamp': timestamp.writeForm() if timestamp else None,
                }
        db = self.db
        db.put(item)


# In[158]:

class EntityMeta(DBOMeta):
    def write(self):
        if self._data:
            raise RuntimeError('Truly, do not write entities with stored fields.')
        if not self.isNew:
            return self
        self.isNew = False
        try:
            self._write(None)
            path = self.path()
            return self
        except:
            self.isNew = True
            raise


# In[159]:

class EventMeta(DBOMeta):
    def write(self, validTime=None, _containers=[]):
        if not self.isNew:
            return self
        self.isNew = False
        try:
            timestamp = Timestamp(v=validTime)
            self._toStoredForm()
            for v in _containers:
                m = _MapElement(v, self, timestamp)
                m.write()
            self._timestamp = timestamp
            self._write(timestamp)
        except:
            self.isNew = True
            raise
            
class _MapElement(object):
    def __init__(self, entity, eventMeta, timestamp):
        self.entity    = entity
        self.eventMeta = eventMeta
        self.timestamp = timestamp
        self.db        = eventMeta.db
    def write(self):
        item = {'entity':    self.entity.meta.path(),
                'event':     self.eventMeta.path(),
                'key':       self.timestamp.indexStr() + '|' + self.eventMeta.path(),
                }
        db = self.db
        db._putMapItem(item)
        return self
    
    


# ## getValue() - return obj.method(args, kwargs) in the current context [Core]
# 
# A low-level routine to return the value of some method call.
# It might return a cached value known to be good. It might compute the value and cache it.

# In[160]:

def getValue(f, a, k):
    # XXX - this doesn't handle methods with args correctly
    obj = a[0]
    name = f.func_name
    key = getattr(obj, name)
    ctx = Context.current()
    v = ctx.get(key)
    if v is not _noVal:
        return v
    v = obj.meta.getField(name)
    if v is not _noVal:
        return v
    v = f(*a, **k)
    if name in obj._storedFields():
        obj.meta.setField(name, v)
    ctx.set(key, v)
    
    return v


# ## A decorator to allow object methods to delegate into the getValue mechanics [Core]

# In[161]:

def node(*a, **k):
    # XXX - this doesn't handle methods with args correctly
    if k:
        def g(*aa, **kk):
            for kw in k:
                assert kw in ('stored',)
            f = aa[0]
            info = k.copy()
            info['name'] = f.func_name
            def fn2(*aaa, **kkk):
                v = getValue(f, aaa, kkk)
                return v
            fn2.nodeInfo = info
            return fn2
        return g
    
    f = a[0]
    def fn(*aa, **kk):
        v = getValue(f, aa, kk)
        return v
    fn.nodeInfo = {'name': f.func_name}
    return fn


# ## A metaclass for our user classes [Core]
# 
# Trivial - we just track the methods that have been tagged as nodes.

# In[162]:

class DBOMetaClass(type):
    def __new__(cls, name, parents, attrs):
        nodes = {}
        for parent in parents:
            if hasattr(parent, '_isDBO'):
                for nf in parent._nodes:
                    nodes[nf['name']] = nf
        for attrname, attrvalue in attrs.iteritems():
            if getattr(attrvalue, 'nodeInfo', 0):
                ni = attrvalue.nodeInfo
                nodes[ni['name']] = ni
                
        nodes = nodes.values()
        
        ret = super(DBOMetaClass, cls).__new__(cls, name, parents, attrs)
        ret._nodes = nodes
        
        return ret


# ## Finally, a client-side Python object that is db-aware... [Core]
# 
# * It's associated with a database
# * It knows how to write itself out to its database
# * We can read it from the database if we know its name
# * It's immutable once created (by contract, anyway)
# * It has a notion of which slots/fields are to be persisted

# In[163]:

class _DBO(object):
    __metaclass__ = DBOMetaClass
    _instanceMetaclass = DBOMeta
    _isDBO = True
    
    def __init__(self, name=None, db=None, **kwargs):
        self._checkStoredFields(kwargs)
        self.meta = self._instanceMetaclass(self, name=name, db=db, kwargs=kwargs)
    
    def _checkStoredFields(self, kwargs):
        stored = self._storedFields()
        bad = []
        for k in kwargs:
            if k not in stored:
                bad.append(k)
        if bad:
            raise RuntimeError('Setting non-stored fields: %s' % ', '.join(bad))
        
    def write(self, **kwargs):
        if self.meta.isNew:
            for n in self._storedFields():
                getattr(self, n)()
        self.meta.write(**kwargs)
        db = self.meta.db
        db.cache[self.meta.path()] = self
        return self

    def _uiFields(self, key=None):
        return [ n['name'] for n in self._nodes ]
        
    @classmethod
    def get(cls, name, db):
        typeId = _tr.name(cls)
        prefix = '/Global/%s/' % typeId
        path = '%s%s' % (prefix, name)
        return db.get(path)
    
    @classmethod
    def _storedFields(cls):
        ret = []
        for n in cls._nodes:
            if n.get('stored'):
                ret.append(n['name'])
        return set(ret)


# ## The Python base classes [Core]
# 
# * **Entity** provides activeEvents(), using the default clock object to determine visibility of events
# * **Event** uses _containers() to determine the entities to which it applies
#  * **DeleteEvent** undoes one or more events
#  * **CancelEvent** undoes a chain of amending events

# In[164]:

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


# In[165]:

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
        return super(Event, self).write(validTime=validTime, _containers=self._containers())
        
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


# In[166]:

def displayTable(objs, names=None):
    if names is None:
        names = []
        for o in objs:
            for n in o._uiFields():
                if n not in names:
                    names.append(n)
    values = []
    for o in objs:
        vals = []
        for n in names:
            fn = getattr(o, n, None)
            if fn:
                try:
                    v = fn()
                    if isinstance(v, _DBO):
                        v = '%s: %s' % (v.__class__.__name__, v.meta.name())
                    else:
                        v = str(v)
                    vals.append(v)
                except:
                    vals.append('*Err*')
            else:
                vals.append(' ')
        values.append(vals)
    t = '|%s|\n' % '|'.join(names)
    t += '|%s|\n' % '|'.join([ '-' for f in names])
    for v in values:
        t += '|%s\n' % '|'.join(v)
    from IPython.display import display, Markdown
    display(Markdown(t))


# ## Clocks for reading [Core]
# 
# Clocks control the visibility of events, and thus the state of entities.
# 
# Clocks are arranged in a tree, with each clock's time defaulting to its parent clock's time.

# In[167]:

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
        
class RootClock(Clock):
    @node
    def cutoffs(self):
        return None
    
    @node
    def parent(self):
        return None


# In[168]:

_tr.add(Clock)
_tr.add(RootClock)
_tr.add(ClockEvent)

_tr.add(DeleteEvent)
_tr.add(CancelEvent)

RootClock('Main', db=_odb).write()

# # Example: Simple Reference Data
# 
# RefData items are just entities with known names. Each event applies to a single refdata item. 
# The last event on a refdata entity holds the current values of that
# data element.
# 

# ## Base RefData Classes [DBA]
# 
# In real life, we'd add some syntactic sugar/helper methods to make it easier on the users of this class.

# In[169]:

class RefData(Entity):

    @node
    def clock(self):
        return Clock.get('RefData', db=self.meta.db)
    
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


# # Customer Reference Data [BA]
# 
# Trivial classes that define customer reference data

# In[170]:

class CustomerRefData(RefData):
    
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
    
class CustomerRefDataUpdateEvent(RefDataUpdateEvent):
    
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


# ## Creating a reference data entity [User]
# 
# Here, we define a single customer that is updated a few times (reflecting initial data,
# a change of company, a fix for a typo, and a last name change.)
# 
# Note that we capture the current system timestamp at various points for use in the next few cells.

# In[171]:

with _odb:
    clock = Clock('RefData').write()

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


# ## Using the reference data [User]
# 
# **info()** prints out some information about our example customer. Note that the function knows nothing about time,
# but it prints different information depending on the cutoffs set on the global clock:
# * If no cutoffs are set, we use the head state of the database
# * If cutoffs are set, we use the state of the database at the timestamp set on the cutoffs

# In[172]:

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
# 
# **_db2** is a new database client, probably in a different process in real life.
# Objects returned from it should be unique to it, even if an object shares the same
# name in our original db.

# In[173]:

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
# 
# 1. We create a new, empty database
# 2. We create a union db of the empty db backed by our 'production' db
# 3. We modify our ref data in the union db (here, just deleting the customer name change)
# 4. We check our union db reflects the change
# 5. We check that our production db is still unchanged
# 
# Note that the new db is a valid db, but it won't be of much use by itself: it's just a helper component for the union
# database.

# In[174]:

print 'Prod db:', _odb._describe()
print

_dbNew = ObjectDb(DynamoDbDriver(ddb))

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
# 
# Here we enter a back-dated correction to our reference data. 
# 
# We can:
# 1. Regenerate the end of day report as it was originally run
# 2. Regenerate the end of day report including late-arriving data (prior day amends.) Note how **e2** is included, 
# but not **e3**.
# 
# Note how this example exposes a weakness in the DBA's design: the last update (the company name change) effectively
# undoes the change of address update. A better design would be to have the update() method only take the modified
# fields, and have the **RefData.state()** method apply the incremental changes from each active event rather than
# just using the complete state in the last event.

# In[175]:

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


# # Example: Help Desk Workflow
# 
# Note this is an implementation of the underlying data model, NOT yet another mess of home-made "languages," plug-ins,
# UIs, and general goop to be sold to naive enterprises.
# 
# The main types are:
# * **Workbook**: A queue of activity
# * **WorkTicket**: A collection of related activity
# * **WorkItem**: An activity item
# * **WorkEvent**: An event related to this workflow system
#     
# In simple cases, tickets and items map one-to-one.
# In more complex cases, tickets may have multiple items (e.g. management an approval item, a legal approval item, etc.)
# 
# Note that WorkItems are double-entry: +1 item in a Workbook will always have an offsetting -1 of the same item in 
# some other workbook. This is important for sanity, auditing, and implementation.

# ## Some dictionary utility functions [Core]
# 
# Little functions to help with aggregation/netting of items

# In[176]:

def merge(d, d2, deleteZeros=True):
    """recursively sum nested dictionaries that eventually have values that are numbers"""
    for k, v in d2.items():
        if isinstance(v, dict):
            if k not in d:
                d[k] = {}
            merge(d[k], v, deleteZeros=deleteZeros)
        else:
            d[k] = d.get(k, 0) + v
        if deleteZeros and not d[k]:
            del d[k]
       
def flatten(d, deleteZeros=True):
    """aggregate 1 level of nested dictionaries"""
    ret = {}
    for v in d.values():
        merge(ret, v, deleteZeros=deleteZeros)
    return ret


# ## Workbook - a queue of stuff that needs attention from the book owner [Core/DBA]
# 
# A ticket is considered in a workbook if it is contributing one or more items to that workbook. No items, no ticket.
# Of course, the event history is available, and as time-travel to see tickets at any point in the past.

# In[177]:

class Workbook(Entity):
    
    @node
    def clock(self):
        return Clock.get('Workflow', db=self.meta.db)
    
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
        return CustomerRefData.get(self.meta.name(), db=self.meta.db)


# ## WorkTicket - an issue that needs resolution [Core/DBA]
# 
# Basically:
# * A ticket gets opened, various events happened to resolve it, then it is closed.
# * A ticket is like a mini-workbook in that multiple events may affect it.
# * A ticket can be in multiple workbooks at the same time (the book that raided the issue, and those resolving it.)

# In[178]:

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
        quantity = self._itemsByBook()[book1][item]
        ev = WorkItemTransferEvent(ticket=self, 
                                   item=item,
                                   quantity=quantity,
                                   book1=book1,
                                   book2=book2, 
                                   action=action, 
                                   db=self.meta.db)
        ev.write()
        return ev


# In[179]:

class WorkItem(Entity):
    pass


# In[180]:

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


# In[181]:

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


# In[182]:

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


# In[183]:

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
        if len(m) > 60:
            m = m[:57] + '...'
        return 'Message: %s' % (m,)

    def _uiFields(self, key=None):
        return ['action', 'message']


# In[184]:

_tr.add(Workbook)
_tr.add(WorkTicket)
_tr.add(WorkItem)
_tr.add(WorkItemOpenEvent)
_tr.add(WorkItemTransferEvent)
_tr.add(WorkItemMessageEvent)


# In[185]:

with _odb: 
    clock = Clock('Workflow').write()
    clock.setParent(RootClock.get('Main', db=_odb))
    
    wb1 = Workbook('Customer123')
    hd  = Workbook('Helpdesk')
    wb3 = Workbook('Customer.joe')
    fd  = Workbook('Fire Department')

    startOfDay = Timestamp()
    
    ev0 = WorkItemOpenEvent(message='Help, I forgot my password',
                            book1=wb1,
                            book2=hd).write()
    
    ev1 = WorkItemOpenEvent(message='Help! My computer is smoking',
                            book1=wb3,
                            book2=hd).write()
    
    noon = Timestamp()
    
    ev2 = WorkItemOpenEvent(message='Help! My computer is on fire!',
                            book1=wb3,
                            book2=hd,
                            ticket=ev1.ticket(),
                            amends=ev1).write()
    
    ev3 = WorkItemOpenEvent(message='My mouse is broken',
                            book1=wb1,
                            book2=hd).write()
    
    t3 = Timestamp()
    
    ticket=ev3.ticket()
    
    ticket.addMessage('Actually, only the right mouse button is bad, so just replace that. Thx!')
    
    ev4 = ev2.ticket().transfer(book1=hd, book2=fd, action='escalate')
    
    ev2.ticket().addMessage('Um, the smoke is making it hard to see my desk.')
    
    t4 = Timestamp()
    
    ev0.cancel()
    
    endOfDay = Timestamp()
    
def status():
    print 'Status:'
    for wb in wb1, wb3, hd, fd:
        print '    Workbook %s:' % wb.meta.name()
        for ticket in wb.tickets():
            print '        %s' % ticket.str(source=wb)
            for event in ticket.activeEvents():
                print '           %s' % event.str(source=wb)
        print


# In[186]:

with Context({clock.cutoffs: startOfDay}) as ctx:
    status()


# In[187]:

with Context({clock.cutoffs: noon}) as ctx:
    status()


# In[188]:

with Context({clock.cutoffs: t3}) as ctx:
    status()


# In[189]:

with Context({clock.cutoffs: t4}) as ctx:
    status()


# In[190]:

with Context({clock.cutoffs: endOfDay}) as ctx:
    status()


# In[191]:

ticket = fd.tickets()[0]

displayTable(ticket._allEvents())
    
print

displayTable(ticket.activeEvents())


# In[192]:

print ticket.sourceBook().refData()

print ticket.sourceBook().clock().str()
print ticket.sourceBook().refData().clock().str()

