
import boto3
from boto3.dynamodb.conditions import Key

# Assumes external process:
#   java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -sharedDb --inMemory

ddb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')
#dc = boto3.client('dynamodb', endpoint_url='http://localhost:8000')

try:
    _dbn
except:
    _dbn = 0
_dbn += 1

class DynamoDbDriver(object):
    def __init__(self, ddb):
        self._ddb = ddb
        
        global _dbn
        
        self.name = 'DDB-MEM-%s' % _dbn

        tables = list(ddb.tables.all())
        names = [ t.table_name for t in tables ]

        while True:        
            _dbn += 1
            entityTableName = 'entities_%s' % _dbn
            mapTableName    = 'map_%s' % _dbn

            if entityTableName not in names:
                break
            
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
     
    def getEntity(self, name):
        return self._entities.get_item(Key={'name': name})
    def putEntity(self, item):
        self._entities.put_item(Item=item)
        
    def getMapEntries(self, entity):
        return self._map.query(KeyConditionExpression=Key('entity').eq(entity.meta.path()))
    def putMapEntry(self, item):
        self._map.put_item(Item=item)
        
    def _describe(self):
        return '%s: entities=%s, map=%s' % (self.name, 
                                            self._entities.item_count, 
                                            self._map.item_count)
