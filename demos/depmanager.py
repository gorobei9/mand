
from mand.core import SimplificationContext, addFootnote, getNode
from mand.graph import DependencyManager

class DM1(DependencyManager):
      
    def __init__(self):
        self.contexts = {}
        super(DM1, self).__init__()
        
    def addDep(self, input, output):
        if not input.key.tweakable:
            if not input.inputs:
                return
        output.inputs.add(input)
        input.outputs.add(output)

    def calculated(self, node):
        if not node.isSimplified:
            return True
        ok = True
        for input in node.floatingTweakPoints():
            addFootnote(text='context simplification failure', info='%s in %s on %s in %s' % (str(node.key), node.ctx.name, str(input.key), input.ctx.name))
            ok = False
        return ok

    def simplify(self, key):
        if key.fullName() == 'RefData:state':
            obj = key.object()
            return (obj.clock().cutoffs,)
        if key.fullName() == 'Portfolio:items':
            obj = key.object()
            obj2 = obj.getObj(_tr.Clock, 'Trading')
            return (obj.clock().cutoffs, obj2.cutoffs)
        if key.fullName() == 'Workbook:items':
            obj = key.object()
            return (obj.clock().cutoffs,)
        
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
                name = 'simp-%s' % len(self.contexts)
                ctx1 = SimplificationContext(tweaks, name)
                self.contexts[cKey] = ctx1
            ctx1 = self.contexts[cKey]
            ret = ctx1.get(key)
            ret.isSimplified = True
        else:
            ret = super(DM1, self).getNode(ctx, key)
            ret.isSimplified = False
        return ret
