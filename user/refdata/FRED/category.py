

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

    def display(self):
        from mand.core import displayListOfDicts, displayHeader, displayMarkdown
        displayHeader(self.name())
        displayMarkdown('* %s sub-categories\n* %s series' % (len(self.childCategoryInfo()),
                                                              len(self.seriesNames())))
        if self.childCategoryInfo():
            displayListOfDicts(self.childCategoryInfo())
        if self.series():
            d = []
            names = [ 'id', 'title', 'popularity', 'frequency', 'units' ]
            for s in self.series():
                info = s.info()
                d.append( dict( [ (n, info[n]) for n in names ] ) )
            d = sorted(d, key=lambda x: (-int(x['popularity']), x['title']))
            displayListOfDicts(d, names=names)
            
_tr.add(FredCategory)
