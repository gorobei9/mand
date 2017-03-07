
import datetime
import dateutil

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
