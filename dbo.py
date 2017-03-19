
from type_registry import _tr
from objmeta import DBOMeta
from monitor import Monitor

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
                ni['key'] = '%s:%s' % (name, ni['name'])
                nodes[ni['name']] = ni
                
        nodes = nodes.values()
        
        ret = super(DBOMetaClass, cls).__new__(cls, name, parents, attrs)
        ret._nodes = nodes
        
        return ret


class _DBO(object):
    __metaclass__ = DBOMetaClass
    _instanceMetaclass = DBOMeta
    _isDBO = True
    _isCosmic = False
    
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

    # not really the right place for this, but so convenient...
    
    def getObj(self, cls, name, create=True):
        try:
            ret = self.meta.db.getObj(cls, name, create=create)
        except:
            print 'Problem getting %s of class %s' % (name, cls)
            raise
        return ret
    
    def getObjs(self, cls, names, create=True):
        return [ self.getObj(cls, name, create=create) for name in names ]
    
    @classmethod
    def get(cls, name, db, create=False):
        typeId = _tr.name(cls)
        prefix = '/Global/%s/' % typeId
        path = '%s%s' % (prefix, name)
        ret = db.get(path)
        if ret is None and create:
            ret = cls(name, db=db)
            if not db.isRO(): # XXX - hmm?
                ret.write()
            else:
                db.cache[path] = ret
        return ret
    
    @classmethod
    def _storedFields(cls):
        ret = []
        for n in cls._nodes:
            if n.get('stored'):
                ret.append(n['name'])
        return set(ret)
