
def merge(d, d2, deleteZeros=True):
    """recursively sum nested dictionaries that eventually have values that are numbers"""
    for k, v in d2.items():
        if isinstance(v, dict):
            if k not in d:
                d[k] = {}
            merge(d[k], v, deleteZeros=deleteZeros)
        else:
            d[k] = d.get(k, 0) + v
        if deleteZeros and not d[k]:
            del d[k]
       
def flatten(d, deleteZeros=True):
    """aggregate 1 level of nested dictionaries"""
    ret = {}
    for v in d.values():
        merge(ret, v, deleteZeros=deleteZeros)
    return ret

"""
def to_short_form(l):
    keys = set()
    for e in l:
        keys.update(e.keys())
    keys = list(keys)
    data = []
    for e in l:
        data.append( [ e.get(k) for k in keys] )
    return keys, data
        
def from_short_form(keys, data):
    ret = []
    for e in data:
        ret.append( dict( [ (k, e[i]) for i, k in enumerate(keys) if e[i] is not None ] ) )
    return ret
"""
