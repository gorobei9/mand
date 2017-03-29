
from dictutils import merge
from node import NodeKey
from utils import displayMarkdown

class DependencyManager(object):
    def __init__(self):
        self.stack = []
        
    # Input management stuff...
    
    def establishDep(self):
        if len(self.stack) > 1:
            output = self.stack[-2]
            input = self.stack[-1]
            self.addDep(input, output)
            self.mergeMeta(input, output)
        else:
            # at top level, display issues, if any...
            input = self.stack[-1]
            if input.footnotes:
                displayMarkdown(input.footnoteMarkdown())
                
    def mergeMeta(self, input, output):
        merge(output.footnotes, input.footnotes, deleteZeros=False)
        output._tweakPoints.update(input._tweakPoints)
        if input.key.tweakable and not input.tweaked:
            output._tweakPoints.add(input)
            
    def addDep(self, input, output):
        output.inputs.add(input)
        input.outputs.add(output)

    def push(self, node):
        self.stack.append(node)
        
    def pop(self):
        self.stack.pop()

    # Context selection stuff...

    def calculated(self, node):
        return True
    
    def getNode(self, ctx, key):
        node = ctx.get(key)
        return node

_dm = DependencyManager()

def dm():
    return _dm

def setDependencyManager(dm):
    global _dm
    _dm = dm

def getNode(bm, ctx=None):
    key = NodeKey.fromBM(bm)
    return getNodeFromKey(key, ctx)

def getNodeFromKey(key, ctx=None):
    if ctx is None:
        obj = key.object()
        from context import Context
        if obj._isCosmic:
            ctx = Context._root()
        else:
            ctx = Context.current()
    node = dm().getNode(ctx, key)
    return node
