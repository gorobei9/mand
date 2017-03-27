
from mand.core import Entity, node, _tr, Monitor
import os
import sys
import json
import xml.etree.ElementTree as ET

if sys.version_info[0] >= 3:
    import urllib.request as url_request
    import urllib.parse as url_parse
    import urllib.error as url_error
else:
    import urllib2 as url_request
    import urllib as url_parse
    import urllib2 as url_error

urlopen = url_request.urlopen
quote_plus = url_parse.quote_plus
urlencode = url_parse.urlencode
HTTPError = url_error.HTTPError

class FredManager(Entity):
    # a hacked version of somebody (github:mortada?) else's code.
    
    max_results_per_request = 1000
    
    @node
    def rootUrl(self):
         return 'https://api.stlouisfed.org/fred'
        
    @node
    def apiKeyFileName(self):
        return '../.fred_api.key'
    
    @node
    def apiKey(self):
        key = os.environ.get('FRED_API_KEY')
        if key:
            return key
        
        api_key_file = self.apiKeyFileName()
        f = open(api_key_file, 'r')
        ret = f.readline().strip()
        return ret
    
    def __fetch_data(self, url):
        _url = url + '&api_key=' + self.apiKey()
        try:
            Monitor.msg('Web', 0, 'urlopen', url=url)
            response = urlopen(_url)
            root = ET.fromstring(response.read())
        except HTTPError as exc:
            root = ET.fromstring(exc.read())
            raise ValueError(root.get('message'))
        return root
    
    def __fetch_data_json(self, url):
        _url = url
        _url += '&api_key=' + self.apiKey()
        _url += '&file_type=json'
        try:
            Monitor.msg('Web', 0, 'urlopen/json', url=url)
            response = urlopen(_url)
            r = response.read()
        except HTTPError as exc:
            root = ET.fromstring(exc.read())
            raise ValueError(root.get('message'))
        ret = json.loads(r)
        return ret

    
    def get_series_info(self, series_id):
        url = "%s/series?series_id=%s" % (self.rootUrl(), series_id)
        root = self.__fetch_data(url)
        info = root[0].attrib
        return info
    
    def get_series_all_releases(self, series_id):
        earliest_realtime_start = '1776-07-04'
        latest_realtime_end = '9999-12-31'
        f = "%s/series/observations?series_id=%s&realtime_start=%s&realtime_end=%s"
        url =  f % (self.rootUrl(),
                    series_id,
                    earliest_realtime_start,
                    latest_realtime_end)
                                                                                        
        root = self.__fetch_data(url)
        data = [ child.attrib for child in root ]
        return data

    def search_category(self, category_id=0):
        url = "%s/category?category_id=%s&" % (self.rootUrl(), category_id)
        return self.__fetch_data_json(url)
        
    def search_category_children(self, category_id=0):
        url = "%s/category/children?category_id=%s&" % (self.rootUrl(), category_id)
        return self.__fetch_data_json(url)
        
    def __do_series_search(self, url):
        root = self.__fetch_data(url)
        num_results_total = int(root.get('count'))  # total number of results,
                                                    # this can be larger than number of results returned
        series_ids = [ child.get('id') for child in root ]
        return series_ids, num_results_total
    
    def __get_search_results(self, url):
        data, num_results_total = self.__do_series_search(url)
        if data is None:
            return data
        max_results_needed = num_results_total
        if max_results_needed > self.max_results_per_request:
            for i in range(1, max_results_needed // self.max_results_per_request + 1):
                offset = i * self.max_results_per_request
                next_data, _ = self.__do_series_search(url + '&offset=' + str(offset))
                data.extend(next_data)
        return data
    
    def search_by_category(self, category_id):
        url = "%s/category/series?category_id=%s&" % (self.rootUrl(), category_id)
        info = self.__get_search_results(url)
        if info is None:
            raise ValueError('No series exists for category id: ' + str(category_id))
        return info
    
_tr.add(FredManager)

