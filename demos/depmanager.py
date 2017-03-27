
from mand.core import Context, addFootnote
from mand.graph import DependencyManager

class DM1(DependencyManager):
    
    def __init__(self):
        self.contexts = {}
        self.simpleClockMethods = ('Workbook:items', 'RefData:state', 'Portfolio:children', 'Portfolio:items')
        super(DM1, self).__init__()
        
    def addDep(self, input, output):
        if not input.key.tweakable:
            if not input.inputs:
                return
        output.inputs.add(input)
        input.outputs.add(output)

    # context simplification:
    
    def calculated(self, node):
        if not node.isSimplified:
            return
        for input in node.inputs:
            if input not in node.ctx.tweaks:
                addFootnote(text='context simplification failure', info='%s on %s' % (str(node.key), str(input.key)))
                print 'tweaks:'
                for t in node.ctx.tweaks:
                    print t
                print 'input:', input
                  
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
        if key.fullName() in self.simpleClockMethods:
            obj = key.object()
            ctx1 = self.getCtx(obj.clock())
            ret = ctx1.get(key)
            ret.isSimplified = True
        else:
            ret = super(DM1, self).getNode(ctx, key)
            ret.isSimplified = False
        return ret
    
