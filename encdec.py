
import json
import zlib
from monitor import Monitor

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
            if e.test:
                if e.test(value):
                    return e.name, e.encode(value)
            else:
                v = e.encode(value)
                if v is not None:
                    return e.name, v
        return None, value
    
    @classmethod
    def decode(cls, name, value, meta):
        # name is the encoder name
        # meta currently only used to get db
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
            lambda v: hasattr(v, '_isDBO'),
            lambda v: _persist(v).meta.path(),
            lambda v, meta: meta.db.get(v)
           )
                   
addEncoding('OL',
            lambda v: isinstance(v, list) and v and hasattr(v[0], '_isDBO'),
            lambda v: [ _persist(o).meta.path() for o in v ],
            lambda v, meta: [ meta.db.get(p) for p in v ]
           )
