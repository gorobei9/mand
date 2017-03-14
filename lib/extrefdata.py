
from mand.core import DBOMetaClass, node
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
        if not ret:
            ret = self._fetchData()
        return ret
    
    def _fetchData(self):
        data = {}
        for name in self._dataFields:
            data[name] = getattr(self, name)._fetcher(self)
        return data
    
    def update(self):
        data = self._fetchData()
        super(ExternalRefData, self).update(**data)
