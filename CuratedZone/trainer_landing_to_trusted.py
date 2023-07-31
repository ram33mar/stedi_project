import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Curated Data
CustomerCuratedData_node1690756716662 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stediproject20230724/customer_curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCuratedData_node1690756716662",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stediproject20230724/step_trainer_landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1",
)

# Script generated for node Drop Duplicate Customer Curated Data
DropDuplicateCustomerCuratedData_node1690756784477 = DynamicFrame.fromDF(
    CustomerCuratedData_node1690756716662.toDF().dropDuplicates(["serialNumber"]),
    glueContext,
    "DropDuplicateCustomerCuratedData_node1690756784477",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1690756847669 = ApplyMapping.apply(
    frame=DropDuplicateCustomerCuratedData_node1690756784477,
    mappings=[
        ("serialNumber", "string", "right_serialNumber", "string"),
        ("birthDay", "string", "birthDay", "string"),
        ("shareWithPublicAsOfDate", "bigint", "shareWithPublicAsOfDate", "bigint"),
        ("shareWithResearchAsOfDate", "bigint", "shareWithResearchAsOfDate", "bigint"),
        ("registrationDate", "bigint", "registrationDate", "bigint"),
        ("customerName", "string", "customerName", "string"),
        ("shareWithFriendsAsOfDate", "bigint", "shareWithFriendsAsOfDate", "bigint"),
        ("email", "string", "email", "string"),
        ("lastUpdateDate", "bigint", "lastUpdateDate", "bigint"),
        ("phone", "string", "phone", "string"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1690756847669",
)

# Script generated for node Join
Join_node1690756814846 = Join.apply(
    frame1=RenamedkeysforJoin_node1690756847669,
    frame2=StepTrainerLanding_node1,
    keys1=["right_serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1690756814846",
)

# Script generated for node Drop Fields
DropFields_node1690756868077 = DropFields.apply(
    frame=Join_node1690756814846,
    paths=[
        "phone",
        "lastUpdateDate",
        "email",
        "shareWithFriendsAsOfDate",
        "customerName",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "shareWithPublicAsOfDate",
        "birthDay",
        "right_serialNumber",
    ],
    transformation_ctx="DropFields_node1690756868077",
)

# Script generated for node Amazon S3
AmazonS3_node1690756998326 = glueContext.getSink(
    path="s3://stediproject20230724/step_trainer trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1690756998326",
)
AmazonS3_node1690756998326.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
AmazonS3_node1690756998326.setFormat("json")
AmazonS3_node1690756998326.writeFrame(DropFields_node1690756868077)
job.commit()
