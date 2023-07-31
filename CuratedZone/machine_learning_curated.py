import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Trusted Data
AccelerometerTrustedData_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stediproject20230724/accelerometer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrustedData_node1",
)

# Script generated for node Step Trainer Trusted Data
StepTrainerTrustedData_node1690765510002 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://stediproject20230724/step_trainer trusted/"],
            "recurse": True,
        },
        transformation_ctx="StepTrainerTrustedData_node1690765510002",
    )
)

# Script generated for node Join Step Trainer Trusted with Accelerometer Trusted
JoinStepTrainerTrustedwithAccelerometerTrusted_node1690765645506 = Join.apply(
    frame1=AccelerometerTrustedData_node1,
    frame2=StepTrainerTrustedData_node1690765510002,
    keys1=["timeStamp"],
    keys2=["sensorReadingTime"],
    transformation_ctx="JoinStepTrainerTrustedwithAccelerometerTrusted_node1690765645506",
)

# Script generated for node Drop PII fields
DropPIIfields_node1690766131488 = DropFields.apply(
    frame=JoinStepTrainerTrustedwithAccelerometerTrusted_node1690765645506,
    paths=["user"],
    transformation_ctx="DropPIIfields_node1690766131488",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node3 = glueContext.getSink(
    path="s3://stediproject20230724/machine_learning_curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="MachineLearningCurated_node3",
)
MachineLearningCurated_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="machine_learning_curated"
)
MachineLearningCurated_node3.setFormat("json")
MachineLearningCurated_node3.writeFrame(DropPIIfields_node1690766131488)
job.commit()
