import json
import json
import boto3
from boto3.dynamodb.conditions import Key
import http.client

tableName = 'Arxiv-recommendations'

# Lambda handler
def lambda_handler(event, context):
    
    # Read input from dynamo --> Parent is - the input paper and Children - list of recommended papers from our models
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(tableName)
    response = table.query(
    # If using only primary key i.e model-name    
    #KeyConditionExpression=Key('Model-name').eq('LSTM'),
    
    # If using primary and sort key i.e model-name and unique-id of paper
    KeyConditionExpression='#model = :model AND #unique = :id',
    ExpressionAttributeNames={
        '#model': 'Model-name',
        '#unique' : 'Unique-id',
    },
    ExpressionAttributeValues={
        ':model': 'LSTM',
        ':id': 'shiraz'
    }
    )
    items = response['Items']

    # For each recommended paper, get total number of co-citations of the paper and write results to dynamo db
    for item in items:
        model_name = item["Model-name"]
        unique_id = item["Unique-id"]
        children = item["Children"]
        parent = item["Parent"]
        results = [];
        grand_total = 0;
        for child in children:
            child_recid, total = get_total_arxiv(parent, child)
            grand_total+=total
            results.append({"arxivid" : child, "Recid" : child_recid, "Total" : total});
        write_to_dynamo(model_name, unique_id, results, grand_total)
    return "Sucess"

# Read input from dynamo db if passing an input payload
def read_from_dynamo(model_name, unique_id):
    dynamoDBResource = boto3.resource('dynamodb')
    table = dynamoDBResource.Table(tableName)
    dynamodb = boto3.client('dynamodb')
    
    response = table.get_item(Key={'Model-name': model_name,'Unique-id': unique_id})
    parent = response['Item']['Parent']
    return parent, response['Item']['Children']

# Write results to dynamo db
def write_to_dynamo(model_name, unique_id, results, grand_total):
    table = boto3.resource('dynamodb').Table(tableName)
    res = table.update_item(
        Key={'Model-name': model_name, 'Unique-id' : unique_id},
      UpdateExpression="SET #results = :results, #grand_total = :grand_total",
      ExpressionAttributeNames={
        '#results': 'Results',
        '#grand_total' : 'Grand_total',
      },
      ExpressionAttributeValues={
        ':results' : results,
        ':grand_total' : grand_total,
      },
    )

# Call inspire API to get metadata of the paper
def get_total_arxiv(parent, child):
    # get recid from arxiv id 
    conn = http.client.HTTPSConnection("inspirehep.net")
    payload = ''
    headers = {}
    conn.request("GET", "/api/arxiv/"+child, payload, headers)
    response = conn.getresponse()
    data = response.read()
    metadata = json.loads(data)
    if "status" in metadata:
        if metadata["status"] == 404:
            return '', 0
    child_recid = str(metadata["metadata"]["control_number"])
    
    conn = http.client.HTTPSConnection("inspirehep.net")
    payload = ''
    headers = {}
    conn.request("GET", "/api/literature?q=refersto%20recid%20"+ parent +"%20and%20refersto%20recid%20"+child_recid, payload, headers)
    response = conn.getresponse()
    data = response.read()
    metadata = json.loads(data)
    return child_recid, metadata["hits"]["total"]
    