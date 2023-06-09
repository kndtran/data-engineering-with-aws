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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1686173394127 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://nugget-lakehouse/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1686173394127",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1686168833434 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://nugget-lakehouse/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerTrusted_node1686168833434",
)

# Script generated for node Join Step Trainer
JoinStepTrainer_node1686169147173 = Join.apply(
    frame1=StepTrainerTrusted_node1686168833434,
    frame2=AccelerometerTrusted_node1686173394127,
    keys1=["sensorReadingTime"],
    keys2=["timeStamp"],
    transformation_ctx="JoinStepTrainer_node1686169147173",
)

# Script generated for node Drop Fields
DropFields_node1686167317564 = DropFields.apply(
    frame=JoinStepTrainer_node1686169147173,
    paths=["user"],
    transformation_ctx="DropFields_node1686167317564",
)

# Script generated for node Step Trainer Curated
StepTrainerCurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1686167317564,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://nugget-lakehouse/step_trainer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerCurated_node3",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1686174049764 = glueContext.write_dynamic_frame.from_catalog(
    frame=DropFields_node1686167317564,
    database="stedi",
    table_name="machine_learning_curated",
    transformation_ctx="MachineLearningCurated_node1686174049764",
)

job.commit()
