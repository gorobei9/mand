
from db import ObjectDb
from encdec import EncDec
from type_registry import _tr
from context import Context
from noval import _noVal
from timestamp import Timestamp
from uuid import getUUID
from monitor import Monitor

class DBOMeta(object):
    def __init__(self, obj, name=None, db=None, kwargs=None):
        if db is None:
            db = ObjectDb.currentDb()
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
                t, s = EncDec.encode(v)
                payload[k] = s
                if t:
                    enc[k] = t
            self._payload = payload
            self._encoding = enc
            
    def getField(self, name):
        if self._payload and name in self._payload:
            p = self._payload[name]
            t = self._encoding.get(name)
            v = EncDec.decode(t, p, self)
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
        if self.typeId.startswith('anon:'):
            raise RuntimeError('trying to persist unregistered class of type %s' % self.typeId)
        Monitor.msg('DB', 1, 'write', metaobj=self)
        path = self.path()
        self._toStoredForm() 
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
        Monitor.msg('DB', -1, 'end', metaobj=self)

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
