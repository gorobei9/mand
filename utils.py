
from printutils import strForm

def displayTable(objs, names=None):
    if names is None:
        names = []
        for o in objs:
            for n in o._uiFields():
                if n not in names:
                    names.append(n)
    values = []
    for o in objs:
        vals = []
        for n in names:
            fn = getattr(o, n, None)
            if fn:
                try:
                    v = fn()
                    if hasattr(v, '_isDBO'):
                        v = '%s: %s' % (v.__class__.__name__, v.meta.name())
                    else:
                        v = str(v)
                    vals.append(v)
                except:
                    vals.append('*Err*')
            else:
                vals.append(' ')
        values.append(vals)
    t = '|%s|\n' % '|'.join(names)
    t += '|%s|\n' % '|'.join([ '-' for f in names])
    for v in values:
        t += '|%s\n' % '|'.join(v)
    from IPython.display import display, Markdown
    display(Markdown(t))

def displayListOfDicts(l, names=None):
    if names is None:
        names = set()
        for e in l:
            names.update(e.keys())
        names = sorted(names)
        
    strs = [ '|%s|' % '|'.join(names),
             '|%s|' % '|'.join([ '-' for f in names])
             ]

    for e in l:
        vals = []
        for n in names:
            s = strForm(e.get(n))
            vals.append(s)
        strs.append( '|%s' % '|'.join(vals) )
    t = '\n'.join(strs)
    from IPython.display import display, Markdown
    display(Markdown(t))
            
    
def displayDict(d):
    names = ['key', 'value']

    strs = [ '|%s|' % '|'.join(names),
             '|%s|' % '|'.join([ '-' for f in names])
             ]
    for k, v in d.items():
        s = strForm(v)
        strs.append( '|%s' % '|'.join([k, s]) )
    t = '\n'.join(strs)
    from IPython.display import display, Markdown
    display(Markdown(t))

def displayMarkdown(md):
    from IPython.display import display, Markdown
    display(Markdown(md))

def displayHeader(txt):
    displayMarkdown('# ' + txt)
