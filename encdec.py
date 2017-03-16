
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

"""
# XXX - This just seems to fight dynamo's encoders...

def comp(v):
    s = json.dumps(v)
    c = s.encode('utf-7').encode('zlib_codec')
    if len(c) < len(s):
        Monitor.msg('Write', 1, 'compress', value='%s to %s' % (len(s), len(c)))
        return c

def decomp(v, meta):
    s = v.decode('zlib_codec').decode('utf-7')
    d = json.loads(s)
    return d

addEncoding('Z',
            None,
            comp,
            decomp)
"""
