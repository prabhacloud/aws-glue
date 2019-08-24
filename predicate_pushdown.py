import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
print "*"*20
print args
print "*"*20

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def print_df(data_frame):
    try:
        print "*"*20
        data_frame.printSchema()
        print "*"*20
        print "Single record:"
        data_frame.show(1)
    except:
        print "Unexpected error:", sys.exc_info()[0]
    pass
    

## @type: DataSource
## @args: [database = "traffic", table_name = "backfill", push_down_predicate = my_partition_predicate, transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
my_partition_predicate = "(year=='2017' and month=='06' and day=='29' and hour == '11')"
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "traffic", table_name = "backfill", push_down_predicate = my_partition_predicate, transformation_ctx = "datasource0")
print_df(datasource0)
print "*"*20

my_partition_predicate = "(year=='2017' and month=='12' and day=='13' and hour == '01')"
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "traffic", table_name = "backfill", push_down_predicate = my_partition_predicate, transformation_ctx = "datasource0")
print_df(datasource0)
print "*"*20

