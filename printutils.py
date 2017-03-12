
def strForm(o, sz=60):
    if isinstance(o, (str, unicode)):
        m = o
    else:
        m = str(o)
    if len(m) > sz:
        m = m[:sz-3] + '...'
    return m
