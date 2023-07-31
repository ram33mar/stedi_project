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

# Script generated for node Customer Trusted S3 Bucket
CustomerTrustedS3Bucket_node1690749444543 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://stediproject20230724/customer_trusted/"],
            "recurse": True,
        },
        transformation_ctx="CustomerTrustedS3Bucket_node1690749444543",
    )
)

# Script generated for node Accelerometer Landing S3 Bucket
AccelerometerLandingS3Bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stediproject20230724/accelerometer_landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLandingS3Bucket_node1",
)

# Script generated for node Drop Duplicates in Customer Data
DropDuplicatesinCustomerData_node1690750844850 = DynamicFrame.fromDF(
    CustomerTrustedS3Bucket_node1690749444543.toDF().dropDuplicates(["email"]),
    glueContext,
    "DropDuplicatesinCustomerData_node1690750844850",
)

# Script generated for node Join Accelerometer with Customer Data
JoinAccelerometerwithCustomerData_node1690749525351 = Join.apply(
    frame1=AccelerometerLandingS3Bucket_node1,
    frame2=DropDuplicatesinCustomerData_node1690750844850,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="JoinAccelerometerwithCustomerData_node1690749525351",
)

# Script generated for node Drop Fields
DropFields_node1690749580047 = DropFields.apply(
    frame=JoinAccelerometerwithCustomerData_node1690749525351,
    paths=[
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
        "serialNumber",
    ],
    transformation_ctx="DropFields_node1690749580047",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://stediproject20230724/accelerometer_trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="accelerometer_trusted"
)
S3bucket_node3.setFormat("json")
S3bucket_node3.writeFrame(DropFields_node1690749580047)
job.commit()
