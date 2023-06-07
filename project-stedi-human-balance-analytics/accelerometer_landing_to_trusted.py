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

# Script generated for node Accelerometer Landing Zone
AccelerometerLandingZone_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://nugget-lakehouse/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLandingZone_node1",
)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1686167185028 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://nugget-lakehouse/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrustedZone_node1686167185028",
)

# Script generated for node Join Customer
JoinCustomer_node1686167092664 = Join.apply(
    frame1=AccelerometerLandingZone_node1,
    frame2=CustomerTrustedZone_node1686167185028,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="JoinCustomer_node1686167092664",
)

# Script generated for node Drop Fields
DropFields_node1686167317564 = DropFields.apply(
    frame=JoinCustomer_node1686167092664,
    paths=[
        "serialNumber",
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

# Script generated for node Trusted Accelerometer Zone
TrustedAccelerometerZone_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1686167317564,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://nugget-lakehouse/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="TrustedAccelerometerZone_node3",
)

job.commit()
