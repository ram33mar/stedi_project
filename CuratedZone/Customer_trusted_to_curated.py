import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Trusted Data
AccelerometerTrustedData_node1690752969704 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://stediproject20230724/accelerometer_trusted/"],
            "recurse": True,
        },
        transformation_ctx="AccelerometerTrustedData_node1690752969704",
    )
)

# Script generated for node Customer Trusted Data
CustomerTrustedData_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stediproject20230724/customer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrustedData_node1",
)

# Script generated for node Join Customer Data with Accelerometer Data
CustomerTrustedData_node1DF = CustomerTrustedData_node1.toDF()
AccelerometerTrustedData_node1690752969704DF = (
    AccelerometerTrustedData_node1690752969704.toDF()
)
JoinCustomerDatawithAccelerometerData_node1690753076184 = DynamicFrame.fromDF(
    CustomerTrustedData_node1DF.join(
        AccelerometerTrustedData_node1690752969704DF,
        (
            CustomerTrustedData_node1DF["email"]
            == AccelerometerTrustedData_node1690752969704DF["user"]
        ),
        "leftsemi",
    ),
    glueContext,
    "JoinCustomerDatawithAccelerometerData_node1690753076184",
)

# Script generated for node Drop Fields
DropFields_node1690753122415 = DropFields.apply(
    frame=JoinCustomerDatawithAccelerometerData_node1690753076184,
    paths=["x", "y", "z", "timeStamp", "user"],
    transformation_ctx="DropFields_node1690753122415",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://stediproject20230724/customer_curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_curated"
)
S3bucket_node3.setFormat("json")
S3bucket_node3.writeFrame(DropFields_node1690753122415)
job.commit()
