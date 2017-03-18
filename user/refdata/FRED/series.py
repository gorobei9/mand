
from mand.lib.extrefdata import ExternalRefData, dataField
from mand.core import node, _tr

class FredSeries(ExternalRefData):
    @node 
    def fredManager(self):
        return self.getObj(_tr.FredManager, 'Main')

    @node
    def fredClock(self):
        return self.getObj(_tr.FredClock, 'Main')
        
    @node
    def releases(self):
        return self.getObj(_tr.FredReleases, self.name())
        
    @node
    def name(self):
        return self.meta.name()
    
    @dataField
    def info(self):
        fm = self.fredManager()
        return fm.get_series_info(self.name())
    
    @node
    def data(self):
        return self.releases().data()
            
    def plot(self):
        import matplotlib.pyplot as plt
        d = self.data()
        days, obs = zip(*d)
        plt.plot_date(x=days, y=obs, marker='.')
        plt.title(self.info()['title'])
        plt.show()

    def display(self):
        from mand.core import displayDict, displayHeader
        displayHeader(self.name())
        displayDict(self.info())
        self.plot()

    """
    ['observation_end', 'last_updated', 'observation_start', 'title', 'seasonal_adjustment_short', 
     'seasonal_adjustment', 'notes', 'popularity', 'realtime_end', 'frequency', 'units_short', 
     'units', 'realtime_start', 'id', 'frequency_short']
    """

_tr.add(FredSeries)
