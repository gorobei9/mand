
from mand.core import Context, addFootnote
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
    
    def getCtx(self, clock):
        ts = clock.cutoffs()
        cKey = (clock, ts) # really want value-based comparison of ts, not object equality
        if cKey not in self.contexts:
            name = 'Simp-%s' % clock.meta.name()
            self.contexts[cKey] = Context({clock.cutoffs: ts}, name)
        return self.contexts[cKey]
            
    def getNode(self, ctx, key):
        n = ctx._get(key)
        if n:
            return n
        ret = super(DM1, self).getNode(ctx, key)
        return ret
    
