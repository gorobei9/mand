
from mand.core import DBOMetaClass, node, EncDec
from mand.lib.refdata import RefData

def dataField(f):
    def fn(self):
        return self.state().get(f.func_name)
    fn._fetcher = f
    fn.func_name = f.func_name
    return fn # node(f)

class ExternalRefDataMetaClass(DBOMetaClass):
    def __new__(cls, name, parents, attrs):
        ret = super(ExternalRefDataMetaClass, cls).__new__(cls, name, parents, attrs)
        dataFields = []
        for attrname, attrvalue in attrs.iteritems():
            if getattr(attrvalue, '_fetcher', None):
                dataFields.append(attrname)
        ret._dataFields = dataFields
        return ret

class ExternalRefData(RefData):
    __metaclass__ = ExternalRefDataMetaClass
    
    @node
    def state(self):
        ret = super(ExternalRefData, self).state()
        # do something sensible for now if this is a new object:
        if ret:
            data = {}
            for k, v in ret.items():
                t, value = v
                data[k] = EncDec.decode(t, value, self.meta)
            return data
        else:
            ret = self._fetchData()
            return ret
    
    def _fetchData(self):
        data = {}
        for name in self._dataFields:
            v = getattr(self, name)._fetcher(self)
            data[name] = v
        return data
    
    def update(self):
        rawData = self._fetchData()
        data = {}
        for k, v in rawData.items():
            data[k] = list(EncDec.encode(v))
        super(ExternalRefData, self).update(**data)

            def _splitLargeData(self, obs, n=1000):
        i = 0
        ret = []
        while i<len(obs):
            ret.append(ExternalDataPage(db=self.meta.db, data=obs[i:i+n]))
            i += n
        return ret
    
    def _joinLargeData(self, pages):
        ret = []
        for p in pages:
            ret.extend(p.data())
        return ret
    
class ExternalDataPage(_DBO):
    
    @node(stored=True)
    def data(self):
        return []
    
_tr.add(ExternalDataPage)
