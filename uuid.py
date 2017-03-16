
# need to fix this now we have persisted dbs...

_uid = 10000
def getUUID():
    global _uid
    _uid = _uid+1
    s = str(_uid)
    return '%02d.%s' % (len(s), s)


