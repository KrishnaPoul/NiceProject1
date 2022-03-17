import json
import boto3
from uuid import uuid4
from datetime import datetime

s3_client=boto3.client('s3')

# lambda function to copy file from 1 s3 to another s3
def lambda_handler(event, context):
    #specify source bucket
    print(event)
    Datetime = str(datetime.now())
    dynamodb = boto3.resource('dynamodb')
    dynamoTable = dynamodb.Table('audio-file-entries')
    source_bucket_name=event['Records'][0]['s3']['bucket']['name']
    #get object that has been uploaded
    file_name=event['Records'][0]['s3']['object']['key']
    #specify destination bucket
    destination_bucket_name='s3-wav-file'
    #specify from where file needs to be copied
    copy_object={'Bucket':source_bucket_name,'Key':file_name}
    #write copy statement 
    s3_client.copy_object(CopySource=copy_object,Bucket=destination_bucket_name,Key=file_name)
    dynamoTable.put_item(Item={'fileName': file_name, 'creationTimestamp': Datetime,'fileState':1})
    s3_client.delete_object(Bucket=source_bucket_name, Key=file_name)
    return {
    'statusCode': 3000,
    'body': json.dumps('File has been Successfully Copied')
    }