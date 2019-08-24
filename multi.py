from awsglue.job import Job
import sys
from pyspark.sql import SQLContext
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
import boto3

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
 
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
glue = boto3.client(service_name='glue',region_name='us-west-2',endpoint_url='https://glue.us-west-2.amazonaws.com')
response=glue.start_job_run(JobName='test-csv-records')
status = glue.get_job(JobName='test-csv-records')
print status['Job']

datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "test", table_name = "geotrucks")
datasource1 = glueContext.create_dynamic_frame.from_catalog(database = "test2", table_name = "geolocation")

df=datasource0.toDF()
df1=datasource1.toDF()

df.createOrReplaceTempView("geotrucks")
df1.createOrReplaceTempView("geolocation")

spark.sql(" SELECT * FROM geotrucks ").show()
spark.sql(" SELECT * FROM geolocation ").show()  

df2=spark.sql(" SELECT * FROM geotrucks b join geolocation a on (a.driverid = b.driverid) ")

dyf2=DynamicFrame.fromDF(df2,glueContext,"dyf1")

glueContext.write_dynamic_frame.from_options(frame = dyf2,
          connection_type = "s3",
          connection_options = {"path": "s3://geo010/geojoin/"},
          format = "csv")

job.commit()