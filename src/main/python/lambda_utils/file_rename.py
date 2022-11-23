import json
import boto3

client = boto3.resource('s3')

print('moving code')

from datetime import date

import boto3
client = boto3.resource('s3')
print('moving code')
bucket = 'finaladobeoutput'

today = date.today()
date_formatted = today.strftime("%Y-%m-%d")
output_file_name = (date_formatted)+'_SearchKeywordPerformance.tab'


my_bucket = client.Bucket(bucket)
for file in my_bucket.objects.all():
    print(file.key)
    if file.key.endswith('csv'):
        final_file_name = file.key
        print (file.key)

elem_split = final_file_name.split('/')
file_name = 'finaladobeoutput/OUTPUT/'+elem_split[len(elem_split)-1]
client.Object('finaladobeoutputupdated',output_file_name).copy_from(CopySource=file_name)

def lambda_handler(event, context):
    pass
