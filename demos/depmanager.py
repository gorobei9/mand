
from mand.core import Context, addFootnote, getNode
from mand.graph import DependencyManager

class DM1(DependencyManager):
    
    def __init__(self):
        self.contexts = {}
        super(DM1, self).__init__()
        
    def addDep(self, input, output):
        #if not input.key.tweakable:
        #    if not input.inputs:
        #        return
        output.inputs.add(input)
        input.outputs.add(output)

    # context simplification:
    
    def calculated(self, node):
        if not node.isSimplified:
            return True
        ok = True
        for input in node.tweakPoints():
            if input not in node.ctx.tweaks:
                addFootnote(text='context simplification failure', info='%s in %s on %s in %s' % (str(node.key), node.ctx.name, str(input.key), input.ctx.name))
                print
                print 'node:', node
                print 'tweaks:'
                for t in node.ctx.tweaks:
                    print t
                print 'input:', input
                ok = False
        return ok

    def simplify(self, key):
        if key.fullName() == 'MarketInterface1:spot':
            obj = key.object()
            
            dep1 = obj.sourceName
            dep2 = obj.source().clock().cutoffs
            
            return (dep1, dep2)
            
    def getNode(self, ctx, key):
        n = ctx._get(key)
        if n:
            return n
        s = self.simplify(key)
        if s: 
            nodes = [ getNode(bm) for bm in s ]
            values = [ bm() for bm in s ]
            k = [ (n.object(), n.key.fullName(), v) for n, v in zip(nodes, values) ]
            cKey = tuple(k)
            
            if cKey not in self.contexts:
                tweaks = dict( [ (bm, v) for bm, v in zip(s, values)])
                ctx1 = Context(tweaks, 'simp')
                self.contexts[cKey] = ctx1
            ctx1 = self.contexts[cKey]
            ret = ctx1.get(key)
            ret.isSimplified = True
        else:
            ret = super(DM1, self).getNode(ctx, key)
            ret.isSimplified = False
        return ret
