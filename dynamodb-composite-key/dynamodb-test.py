import boto3

tablename = 'LogPython'

client = boto3.client('dynamodb', endpoint_url='http://localhost:8000')

# Delete table if it exists, then create
try:
    client.delete_table(TableName=tablename)
except client.exceptions.ResourceNotFoundException:
    #NOOP: table didn't exist
    pass 

client.create_table(TableName=tablename, 
    AttributeDefinitions=[{"AttributeName":"Id","AttributeType":"S"},{"AttributeName":"EventDate","AttributeType":"S"}], 
    KeySchema=[{"AttributeName":"Id","KeyType":"HASH"}, {"AttributeName":"EventDate", "KeyType":"RANGE"}],
    ProvisionedThroughput={"ReadCapacityUnits":5, "WriteCapacityUnits":5})

client.put_item(TableName=tablename,Item={"Id": {"S":"rloc:1"},"EventDate":{"S":"12345678"}})
client.put_item(TableName=tablename,Item={"Id": {"S":"rloc:1"},"EventDate":{"S":"12345679"}})


# Count items
response = client.query(TableName=tablename, Select='COUNT', 
    KeyConditionExpression='Id = :id AND EventDate BETWEEN :startdate AND :enddate',
    ExpressionAttributeValues={":id": {"S":"rloc:1"}, ":startdate": {"S":"12345670"}, ":enddate": {"S":"12345680"}})

if "LastEvaluatedKey" in response:
    raise Exception("Looks like we need to implement paging for the query")

print(response)