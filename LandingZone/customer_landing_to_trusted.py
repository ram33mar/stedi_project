import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Landing S3 Bucket
CustomerLandingS3Bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stediproject20230724/customer_landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLandingS3Bucket_node1",
)

# Script generated for node Privacy Filter
PrivacyFilter_node1690748975800 = Filter.apply(
    frame=CustomerLandingS3Bucket_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="PrivacyFilter_node1690748975800",
)

# Script generated for node Customer Trusted Data
CustomerTrustedData_node3 = glueContext.getSink(
    path="s3://stediproject20230724/customer_trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerTrustedData_node3",
)
CustomerTrustedData_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_trusted"
)
CustomerTrustedData_node3.setFormat("json")
CustomerTrustedData_node3.writeFrame(PrivacyFilter_node1690748975800)
job.commit()
