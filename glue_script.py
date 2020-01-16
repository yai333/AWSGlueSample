import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import StringType

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "curo-staging", table_name = "curo_prod_public_activity_activity", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "curo-staging", table_name = "curo_prod_public_activity_activity", transformation_ctx = "datasource0")

## convert datasource to pyspark dataframe
df=datasource0.toDF()

## create new colume cast jsonb to string
df = df.withColumn("metadata_string",  df["metadata"].cast(StringType()))

## drop jsonb column
df = df.drop("metadata")

## rename metadata_string to metadata
df = df.withColumnRenamed('metadata_string', 'metadata')

## convert pyspark dataframe to glue dynamic frame
datasource2 = datasource0.fromDF(df, glueContext, "datasource2")

## @type: ApplyMapping
## @args: [mapping = [("result_type", "string", "result_type", "string"), ("metadata", "string", "metadata", "string"), ("device_id", "int", "device_id", "int"), ("task_id", "int", "task_id", "int"), ("title", "string", "title", "string"), ("message", "string", "message", "string"), ("uuid", "string", "uuid", "string"), ("is_synced", "boolean", "is_synced", "boolean"), ("hub_status", "string", "hub_status", "string"), ("event_id", "int", "event_id", "int"), ("hub_id", "int", "hub_id", "int"), ("group_id", "int", "group_id", "int"), ("user_id", "int", "user_id", "int"), ("subtitle", "string", "subtitle", "string"), ("id", "int", "id", "int"), ("time", "timestamp", "time", "timestamp")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource2]
applymapping1 = ApplyMapping.apply(frame = datasource2, mappings = [("result_type", "string", "result_type", "string"), ("metadata", "string", "metadata", "string"), ("device_id", "int", "device_id", "int"), ("task_id", "int", "task_id", "int"), ("title", "string", "title", "string"), ("message", "string", "message", "string"), ("uuid", "string", "uuid", "string"), ("is_synced", "boolean", "is_synced", "boolean"), ("hub_status", "string", "hub_status", "string"), ("event_id", "int", "event_id", "int"), ("hub_id", "int", "hub_id", "int"), ("group_id", "int", "group_id", "int"), ("user_id", "int", "user_id", "int"), ("subtitle", "string", "subtitle", "string"), ("id", "int", "id", "int"), ("time", "timestamp", "time", "timestamp")], transformation_ctx = "applymapping1")
## @type: ResolveChoice
## @args: [choice = "make_struct", transformation_ctx = "resolvechoice2"]
## @return: resolvechoice2
## @inputs: [frame = applymapping1]
resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_struct", transformation_ctx = "resolvechoice2")
## @type: DropNullFields
## @args: [transformation_ctx = "dropnullfields3"]
## @return: dropnullfields3
## @inputs: [frame = resolvechoice2]
dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")
## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://curo-glue-datalake/staging"}, format = "parquet", transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = dropnullfields3]
datasink4 = glueContext.write_dynamic_frame.from_options(frame = dropnullfields3, connection_type = "s3", connection_options = {"path": "s3://curo-glue-datalake/staging"}, format = "parquet", transformation_ctx = "datasink4")
job.commit()
