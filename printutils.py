
def strForm(o, sz=60):
    m = str(o)
    if len(m) > sz:
        m = m[:sz-3] + '...'
    return m
