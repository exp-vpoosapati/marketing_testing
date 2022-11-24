"""
-Name (Name of Spark cluster)
-LogUri (S3 bucket to store EMR logs)
-Ec2SubnetId (The subnet to launch the cluster into)
-JobFlowRole (Service role for EC2)
-ServiceRole (Service role for Amazon EMR)

The following parameters are additional parameters for the Spark job itself. Change the bucket name and prefix for the Spark job (located at the bottom).

-s3://your-bucket-name/prefix/lambda-emr/SparkProfitCalc.jar (Spark jar file)
-s3://your-bucket-name/prefix/fake_sales_data.csv (Input data file in S3)
-s3://your-bucket-name/prefix/outputs/report_1/ (Output location in S3)

"""
import json
import boto3


client = boto3.client('emr')
s3Client = boto3.client('s3')

def lambda_handler(event, context):

    print (event)
    print (context)

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    print ('bucket')
    print (bucket)
    print ('key')
    print (key)

    file_name_input = 's3://'+str(bucket)+'/'+str(key)

    print (file_name_input)

    response = client.run_job_flow(
        Name= 'spark_job_cluster',
        LogUri= 's3://emrlogsexamples/logs/',
        ReleaseLabel= 'emr-5.36.0',
        Instances={
            'MasterInstanceType': 'm5.xlarge',
            'SlaveInstanceType': 'm5.large',
            'InstanceCount': 1,
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False,
            'Ec2SubnetId': 'subnet-037e517b18202af30'
        },
        Applications = [ {'Name': 'Spark'} ],
        Configurations = [
            { 'Classification': 'spark-hive-site',
              'Properties': {
                  'hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'}
            }
        ],
        VisibleToAllUsers=True,
        JobFlowRole = 'EMR_EC2_DefaultRole',
        ServiceRole = 'EMR_DefaultRole',
        Steps=[
            {
                'Name': 'adobe_marketing_data',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': [
                            'spark-submit',
                            '--deploy-mode', 'cluster',
                            's3://dataholdstore/adobe_test_run/Adobe/src/main/python/RUNNER/run_marketing_data_aws.py',
                            file_name_input
                        ]
                }
            }
        ]
    )

    print ('final response')
    print (response)
