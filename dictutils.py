
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

