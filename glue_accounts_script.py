import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import time
import datetime
from pyspark.sql import functions as spark_f
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "financialservicesrds", table_name = "accounts", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "financialservicesrds", table_name = "accounts", transformation_ctx = "datasource0")


#def AddTimestamp(rec):
#  rec["createddate"] = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
#  return rec
#applytimestamp = Map.apply(frame = datasource0, f = AddTimestamp)

## @type: ApplyMapping
## @args: [mapping = [("accountnumber", "long", "accountnumber", "long"), ("customerid", "long", "customerid", "long"), ("type", "string", "type", "string"), ("balance", "double", "balance", "double"), ("createddate", "timestamp", "createddate", "timestamp"), ("active", "tinyint", "active", "tinyint")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
df = datasource0.toDF()

def date_convt(dt):
    if dt is None:
        return

    return spark_f.to_timestamp(dt, 'yyyy-MM-dd HH:mm:ss')

modified_df = df.withColumn("createddate", date_convt(df["createddate"]))
modified_dyf = DynamicFrame.fromDF(modified_df, glueContext, "modified_dyf")
applymapping1 = ApplyMapping.apply(frame = modified_dyf, mappings = [("accountnumber", "long", "accountnumber", "long"), ("customerid", "long", "customerid", "long"), ("type", "string", "type", "string"), ("balance", "double", "balance", "double"), ("createddate", "timestamp", "createddate", "timestamp"), ("active", "int", "active", "tinyint")], transformation_ctx = "applymapping1")



## @type: ResolveChoice
## @args: [choice = "make_cols", transformation_ctx = "resolvechoice2"]
## @return: resolvechoice2
## @inputs: [frame = applymapping1]
resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_cols", transformation_ctx = "resolvechoice2")
## @type: DropNullFields
## @args: [transformation_ctx = "dropnullfields3"]
## @return: dropnullfields3
## @inputs: [frame = resolvechoice2]
dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")
## @type: DataSink
## @args: [catalog_connection = "financialservices-rds-con", connection_options = {"dbtable": "accounts", "database": "financialservicedb"}, transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = dropnullfields3]
datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dropnullfields3, catalog_connection = "financialservices-rds-con", connection_options = {"dbtable": "accounts", "database": "financialservicedb"}, transformation_ctx = "datasink4")
job.commit()
