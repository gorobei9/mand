
from .graph import _getCurrentNode
from .dictutils import merge

def addFootnote(text=None,
                info=None,
                node=None):
    key = text
    if node is None:
        node = _getCurrentNode()

    s = set([info]) if info else set()
    merge(node.footnotes, { key: s }, deleteZeros=False)
