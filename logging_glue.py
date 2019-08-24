import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import time
 
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
 
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
client = boto3.client('logs',region_name='us-west-2')
def create_log_stream(log_group,job_run_id):
    response = client.create_log_stream(
        logGroupName=log_group,
        logStreamName=job_run_id
    )
    return response
def get_sequence_token(log_group,job_run_id):
    response = client.describe_log_streams(
    logGroupName=log_group,
    logStreamNamePrefix=job_run_id
    )
    if 'uploadSequenceToken' in response['logStreams'][0]:
        return response['logStreams'][0]['uploadSequenceToken']
    else:
        return None
def put_events(log_group,job_run_id,log_message):
    #Custom log event will be inserted and each sequenceToken will be retrieved using describe_log_streams
    response = client.put_log_events(logGroupName=log_group,logStreamName=job_run_id,logEvents=[{ 'timestamp': int(round(time.time() * 1000)),'message': log_message}],sequenceToken=get_sequence_token(log_group,job_run_id))
    return response

if __name__ == '__main__':
    log_group='/aws-glue/jobs/output'
    job_name=args['JOB_NAME']
    job_run_id=args['JOB_RUN_ID']
    custom_log=job_run_id+'_Custom'
    #create logstream
    create_log_stream(log_group,custom_log)
    response=client.describe_log_streams(logGroupName=log_group,limit=1,orderBy='LastEventTime',descending=True)
    if response['logStreams'][0]['logStreamName'] == job_run_id :
        print "Successfully created logStream"+custom_log
    # Initial insertion into the Log stream as there is not Sequence token present already
    response = client.put_log_events(logGroupName=log_group,logStreamName=custom_log,logEvents=[{ 'timestamp': int(round(time.time() * 1000)),'message': 'BEGINNING OF CUSTOM LOGGING'}])
    time.sleep(10)
    put_events(log_group,custom_log,'MIDDLE OF CUSTOM LOGGING')
    time.sleep(10)
    put_events(log_group,custom_log,'END OF CUSTOM LOGGING')

job.commit()