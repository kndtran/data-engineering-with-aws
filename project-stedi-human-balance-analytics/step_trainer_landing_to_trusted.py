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

# Script generated for node Customer Curated Zone
CustomerCuratedZone_node1686167185028 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://nugget-lakehouse/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCuratedZone_node1686167185028",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1686168833434 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://nugget-lakehouse/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1686168833434",
)

# Script generated for node Join Step Trainer
JoinStepTrainer_node1686169147173 = Join.apply(
    frame1=CustomerCuratedZone_node1686167185028,
    frame2=StepTrainerLanding_node1686168833434,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="JoinStepTrainer_node1686169147173",
)

# Script generated for node Drop Fields
DropFields_node1686167317564 = DropFields.apply(
    frame=JoinStepTrainer_node1686169147173,
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
    ],
    transformation_ctx="DropFields_node1686167317564",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1686167317564,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://nugget-lakehouse/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrusted_node3",
)

# Script generated for node Step Trainer Trusted Table
StepTrainerTrustedTable_node1686174608857 = (
    glueContext.write_dynamic_frame.from_catalog(
        frame=DropFields_node1686167317564,
        database="stedi",
        table_name="step_trainer_trusted",
        transformation_ctx="StepTrainerTrustedTable_node1686174608857",
    )
)

job.commit()
