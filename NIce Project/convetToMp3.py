import boto3
import json
from datetime import datetime
def lambda_handler(event, context):
    print(event)
    Datetime = str(datetime.now())
    body=event['Records'][0]['body']
    json_object = json.loads(body)
    print(json_object["Records"][0]["s3"]["object"]["key"])
    s3 = boto3.client('s3')
    s31 = boto3.resource('s3')
    dynamodb = boto3.resource('dynamodb')
    dynamoTable = dynamodb.Table('audio-file-entries')
    bucket =json_object["Records"][0]["s3"]["bucket"]["name"]
    prefix = ''
    suffix = 'wav'
    kwargs = {'Bucket': bucket}
    if isinstance(prefix, str):
        kwargs['Prefix'] = prefix
    list=[]
    resp = s3.list_objects_v2(**kwargs)
    contents = resp['Contents']
    for con in contents:
        if con['Key'].endswith(suffix):
            list.append(con['Key'])
            dynamoTable.update_item(Key={'fileName':json_object["Records"][0]["s3"]["object"]["key"]},UpdateExpression="SET compressionTimestamp = :r,fileState=:s",ExpressionAttributeValues={':r': Datetime,':s':2},ReturnValues="UPDATED_NEW")
            copy_source = {'Bucket': bucket,'Key': con['Key']}
            s31.meta.client.copy(copy_source, 's3-mp3-file', con['Key'].split('.')[0]+'.mp3')
            dynamoTable.update_item(Key={'fileName':json_object["Records"][0]["s3"]["object"]["key"]},UpdateExpression="SET deletionTimestamp = :r,fileState= :s",ExpressionAttributeValues={':r': Datetime,':s':3},ReturnValues="UPDATED_NEW")
            s3.delete_object(Bucket=bucket, Key=con['Key'])
    
    
    