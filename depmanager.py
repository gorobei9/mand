
from dictutils import merge

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
            
    def mergeMeta(self, input, output):
        merge(output.footnotes, input.footnotes, deleteZeros=False)
        output._tweakPoints.update(input.tweakPoints())

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

