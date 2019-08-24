import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import time 
#from awsglue.transforms import Relationalize
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

glue_jsonarray_input_s3_path="s3://test/json-test"
glue_temp_storage = "s3://test/json-csv2"
glue_flattened_output_s3_path = "s3://test/json-csv3"
dfc_root_table_name = "root"

glueContext = GlueContext(spark.sparkContext)
datasource0 = glueContext.create_dynamic_frame.from_options(connection_type="s3", connection_options={"paths":[glue_jsonarray_input_s3_path]}, format="json", format_options={"jsonPath": "$[*]"}, transformation_ctx="datasource0")
df_collection = Relationalize.apply(frame = datasource0, staging_path = glue_temp_storage, name = dfc_root_table_name, transformation_ctx = "df_collection")
blogdata = df_collection.select(dfc_root_table_name)
print "testing"
print blogdata.printSchema()
print blogdata.show()
print blogdata.count()
blogdataoutput = glueContext.write_dynamic_frame.from_options(frame = blogdata, connection_type = "s3", connection_options = {"path": glue_flattened_output_s3_path}, format = "csv", transformation_ctx = "blogdataoutput")