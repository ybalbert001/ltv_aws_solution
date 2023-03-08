#!/usr/bin/env python3

import aws_cdk as cdk

from aws_cdk import (
  Stack,
  aws_s3 as s3,
  aws_s3_deployment as s3deploy
)
from constructs import Construct

class S3Stack(Stack):

  def __init__(self, scope: Construct, construct_id: str, resource_suffix, **kwargs) -> None:
    super().__init__(scope, construct_id, **kwargs)

    target_bucket_name = 'ltv-mwaa-{}'.format(resource_suffix)
    s3_bucket = s3.Bucket(self, "CreateMWAAS3Bucket", bucket_name=target_bucket_name, block_public_access=s3.BlockPublicAccess.BLOCK_ALL, removal_policy=cdk.RemovalPolicy.DESTROY)

    data_bucket_name = 'ltv-data-{}'.format(resource_suffix)
    data_s3_bucket = s3.Bucket(self, "CreateDataS3Bucket", bucket_name=data_bucket_name, block_public_access=s3.BlockPublicAccess.BLOCK_ALL, removal_policy=cdk.RemovalPolicy.DESTROY)

    deployment = s3deploy.BucketDeployment(self, "DeployMWAAasset",
        sources=[s3deploy.Source.asset("../mwaa-etl")],
        destination_key_prefix='mwaa-etl/',
        destination_bucket=s3_bucket
    )

    # ConstructThatReadsFromTheBucket(self, "Consumer", {
    #     # Use 'deployment.deployedBucket' instead of 'websiteBucket' here
    #     "bucket": deployment.deployed_bucket
    # })

    self.s3_bucket = s3_bucket
    self.data_s3_bucket = data_s3_bucket

    cdk.CfnOutput(self, 'Mwaa-S3', value=s3_bucket.bucket_name, export_name='Mwaa-S3')
    cdk.CfnOutput(self, 'Data-S3', value=data_s3_bucket.bucket_name, export_name='Data-S3')