import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import time
import datetime

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "financialservicesrds", table_name = "customers", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "financialservicesrds", table_name = "customers", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("customerid", "long", "customerid", "long"), ("firstname", "string", "firstname", "string"), ("lastname", "string", "lastname", "string"), ("businessname", "string", "businessname", "string"), ("phone", "string", "phone", "string"), ("email", "string", "email", "string"), ("addressline1", "string", "addressline1", "string"), ("addressline2", "string", "addressline2", "string"), ("city", "string", "city", "string"), ("state", "string", "state", "string"), ("country", "string", "country", "string"), ("customergrouping", "string", "customergrouping", "string"), ("ownertype", "string", "ownertype", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]

def AddTimestamp(rec):
  rec["updateddate"] = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
  return rec

modifiedf =  Map.apply(frame = datasource0, f = AddTimestamp)

applymapping1 = ApplyMapping.apply(frame = modifiedf, mappings = [("customerid", "long", "customerid", "long"), ("firstname", "string", "firstname", "string"), ("lastname", "string", "lastname", "string"), ("businessname", "string", "businessname", "string"), ("phone", "string", "phone", "string"), ("email", "string", "email", "string"), ("addressline1", "string", "addressline1", "string"), ("addressline2", "string", "addressline2", "string"), ("city", "string", "city", "string"), ("state", "string", "state", "string"), ("country", "string", "country", "string"), ("customergrouping", "string", "customergrouping", "string"), ("ownertype", "string", "ownertype", "string"), ("updateddate", "string", "updateddate", "timestamp")], transformation_ctx = "applymapping1")
#applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("customerid", "long", "customerid", "long"), ("firstname", "string", "firstname", "string"), ("lastname", "string", "lastname", "string"), ("businessname", "string", "businessname", "string"), ("phone", "string", "phone", "string"), ("email", "string", "email", "string"), ("addressline1", "string", "addressline1", "string"), ("addressline2", "string", "addressline2", "string"), ("city", "string", "city", "string"), ("state", "string", "state", "string"), ("country", "string", "country", "string"), ("customergrouping", "string", "customergrouping", "string"), ("ownertype", "string", "ownertype", "string"), ("updateddate", "string", "updateddate", "timestamp")], transformation_ctx = "applymapping1")

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
## @args: [catalog_connection = "financialservices-rds-con", connection_options = {"dbtable": "customers", "database": "financialservicedb"}, transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = dropnullfields3]
datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dropnullfields3, catalog_connection = "financialservices-rds-con", connection_options = {"dbtable": "customers", "database": "financialservicedb"}, transformation_ctx = "datasink4")
job.commit()
