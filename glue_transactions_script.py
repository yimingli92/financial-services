import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as spark_f
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, lit

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "financialservicesrds", table_name = "transactions", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "financialservicesrds", table_name = "transactions", transformation_ctx = "datasource0")

## @type: ApplyMapping
## @args: [mapping = [("transactionid", "long", "transactionid", "long"), ("customerid", "long", "customerid", "long"), ("amount", "double", "amount", "double"), ("creditordebit", "long", "creditordebit", "long"), ("currency", "string", "currency", "string"), ("createddate", "string", "createddate", "string"), ("completeddate", "string", "completeddate", "string"), ("cancelleddate", "string", "cancelleddate", "string"), ("rejecteddate", "string", "rejecteddate", "string"), ("status", "string", "status", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
def date_convt(dt):
    if dt is None:
        return

    return spark_f.to_timestamp(dt, 'yyyy-MM-dd HH:mm:ss')

df = datasource0.toDF()
modified_df = df.withColumn("createddate", date_convt(df['createddate'])).withColumn("completeddate", date_convt(df['completeddate'])).withColumn("cancelleddate", date_convt(df['cancelleddate'])).withColumn("rejecteddate", date_convt(df["rejecteddate"]))

modified_dyf = DynamicFrame.fromDF(modified_df, glueContext, "modified_dyf")

applymapping1 = ApplyMapping.apply(frame = modified_dyf, mappings = [("transactionid", "long", "transactionid", "long"), ("customerid", "long", "customerid", "long"), ("amount", "double", "amount", "double"), ("creditordebit", "long", "creditordebit", "long"), ("currency", "string", "currency", "string"), ("createddate", "timestamp", "createddate", "timestamp"), ("completeddate", "timestamp", "completeddate", "timestamp"), ("cancelleddate", "timestamp", "cancelleddate", "timestamp"), ("rejecteddate", "timestamp", "rejecteddate", "timestamp"), ("status", "string", "status", "string")], transformation_ctx = "applymapping1")
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
## @args: [catalog_connection = "financialservices-rds-con", connection_options = {"dbtable": "transactions", "database": "financialservicedb"}, transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = dropnullfields3]
datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dropnullfields3, catalog_connection = "financialservices-rds-con", connection_options = {"dbtable": "transactions", "database": "financialservicedb"}, transformation_ctx = "datasink4")
job.commit()
