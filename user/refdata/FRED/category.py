

from mand.lib.extrefdata import ExternalRefData, dataField
from mand.core import node, _tr

class FredCategory(ExternalRefData):
    @node 
    def fredManager(self):
        return self.getObj(_tr.FredManager, 'Main')
    
    @node
    def id(self):
        return self.meta.name()
    
    @dataField
    def info(self):
        fm = self.fredManager()
        return fm.search_category(self.id())
    
    @dataField
    def childCategoryInfo(self):
        fm = self.fredManager()
        ret = fm.search_category_children(self.id())
        return ret.get('categories')
      
    @dataField
    def seriesNames(self):
        fm = self.fredManager()
        return fm.search_by_category(self.id())
    
    @node
    def name(self):
        info = self.info()
        return info['categories'][0]['name']
    
    @node
    def childCategoryNames(self):
       return [ c['id'] for c in self.childCategoryInfo() ] 

    @node
    def children(self):
       return self.getObjs(_tr.FredCategory, self.childCategoryNames())
    
    @node
    def series(self):
        return self.getObjs(_tr.FredSeries, self.seriesNames())
    
_tr.add(FredCategory)
