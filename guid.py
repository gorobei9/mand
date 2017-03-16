
# really want short guid that are:
#  unique at the physical db/class level so unions work without risk of shadowing names

import uuid

def getUUID():
    return uuid.uuid4()



