
import boto3
from boto3.dynamodb.conditions import Key

# Assumes external process:
#   java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -sharedDb --inMemory
#   java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -sharedDb --port 8001

ddb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')
#dc = boto3.client('dynamodb', endpoint_url='http://localhost:8000')

class DynamoDbDriver(object):

    ddb_mem = None
    ddb_prod = None
    
    def __init__(self, foo=None, name=None, inMem=True, ro=None):
        # I actually learned this idiom from Google's Tensorflow code. Thanks, guys!
        # If you switch from a boa constructor to a keyword constructor, stick a
        # dummy named parameter at the front: calls using the old signature can be failed early.
        assert foo is None

        if ro is None:
            ro = not inMem
            
        self.ro     = ro
        self.inMem  = inMem
        self.anon   = name is None
        
        if name is None:
            assert inMem
            name = self.anon_name()

        self.name = name
        
        if not self.createTables(name):
            ddb = self.ddb()
            self._entities = ddb.Table(self.entityTableName()) 
            self._map      = ddb.Table(self.mapTableName())

    def entityTableName(self, name=None):
        if name is None:
            name = self.name
        return 'entities_%s' % name
        
    def mapTableName(self, name=None):
        if name is None:
            name = self.name
        return 'map_%s' % name
    
    def ddb(self):
        if self.inMem:
            if self.ddb_mem is None:
                self.ddb_mem = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')
            return self.ddb_mem
        else:
            if self.ddb_prod is None:
                self.ddb_prod = boto3.resource('dynamodb', endpoint_url='http://localhost:8001')
            return self.ddb_prod

    
    def anon_name(self):
        ddb = self.ddb()

        tables = list(ddb.tables.all())
        names = set([ t.table_name for t in tables ])

        name = 0
        while True:        
            name += 1
            entityTableName = self.entityTableName(name)
            if entityTableName not in names:
                return str(name)
   
    def createTables(self, name):
        if self.ro:
            return False
        
        entityTableName = self.entityTableName(name)
        mapTableName    = self.mapTableName(name)

        ddb = self.ddb()

        tables = list(ddb.tables.all())
        names = [ t.table_name for t in tables ]
        if entityTableName in names:
            return False
        
        self._entities = ddb.create_table(
            TableName             = entityTableName,
            KeySchema             = [ { 'AttributeName': 'name', 'KeyType': 'HASH' }, ],
            AttributeDefinitions  = [ { 'AttributeName': 'name', 'AttributeType': 'S' }, ],
            ProvisionedThroughput = { 'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5 }
            )
        self._entities.meta.client.get_waiter('table_exists').wait(TableName=entityTableName)

        self._map = ddb.create_table(
            TableName             = mapTableName,
            KeySchema             = [ {'AttributeName': 'entity', 'KeyType': 'HASH'},
                                      {'AttributeName': 'key',    'KeyType': 'RANGE'}],
            AttributeDefinitions  = [ {'AttributeName': 'entity', 'AttributeType': 'S'},
                                      {'AttributeName': 'key',    'AttributeType': 'S'},],
            ProvisionedThroughput = {'ReadCapacityUnits': 5,'WriteCapacityUnits': 5}
            )
        self._map.meta.client.get_waiter('table_exists').wait(TableName=mapTableName)
        return True
    
    def getEntity(self, name):
        return self._entities.get_item(Key={'name': name})
    
    def putEntity(self, item):
        assert not self.ro
        self._entities.put_item(Item=item)
        
    def getMapEntries(self, entity):
        return self._map.query(KeyConditionExpression=Key('entity').eq(entity.meta.path()))
    
    def putMapEntry(self, item):
        assert not self.ro
        self._map.put_item(Item=item)
        
    def _describe(self):
        return '%s, mem=%s, ro=%s: entities=%s, map=%s' % (self.name,
                                                           self.inMem,
                                                           self.ro,
                                                           self._entities.item_count, 
                                                           self._map.item_count)
