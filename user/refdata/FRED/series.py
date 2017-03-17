
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
    def name(self):
        return self.meta.name()
    
    @dataField
    def info(self):
        fm = self.fredManager()
        return fm.get_series_info(self.name())
    
    @dataField
    def allReleases(self):
        fm = self.fredManager()
        obs = fm.get_series_all_releases(self.name())
        return self._splitLargeData(obs, 1000)
        
    @node 
    def allObservations(self):
        pages = self.allReleases()
        return self._joinLargeData(pages)
    
    @node
    def data(self):
        # In real life, we would depend on a custom date/date timestamp for observation visibility
        import datetime
        def parseDate(str):
            return datetime.datetime.strptime(str, '%Y-%m-%d').date()
        vis = {}
        updated = {}
        
        cutoffs = self.fredClock().cutoffs()
        
        for record in self.allObservations():
            value = record['value']
            if value == '.':
                continue
            observationDate = parseDate(record['date'])
            updateDate = parseDate(record['realtime_start'])
            value = float(value)
            if cutoffs:
                if observationDate > cutoffs.validDate:
                    continue
                if updateDate > cutoffs.transactionDate:
                    contine
            if observationDate not in vis or updated[observationDate] < updateDate:
                vis[observationDate] = value
                updated[observationDate] = updateDate
        return sorted(vis.items()) # that's so conservative it's virtually paranoid. 
                                   # I bet series is always in order.
            
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
