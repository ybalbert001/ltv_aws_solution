#!/usr/bin/env python3
import os
import random
import string

import aws_cdk as cdk

from cdk.cdk_stack import CdkStack
from cdk.cdk_vpc import VpcStack
from cdk.cdk_mwaa import MwaaStack
from cdk.cdk_s3 import S3Stack

app = cdk.App()

suffix_str = app.node.try_get_context('suffix').lower()

account_str = app.node.try_get_context('account') #687752207838

resource_suffix = suffix_str if suffix_str else ''.join(random.sample((string.ascii_letters), k=10)).lower()

AWS_ENV = cdk.Environment(account=account_str, region='us-east-1')

vpc_stack = VpcStack(app, "VpcStack", env=AWS_ENV)

ltv_stack = CdkStack(app, "CdkStack", vpc_stack.redshift_vpc, resource_suffix, env=AWS_ENV)

s3_stack = S3Stack(app, "S3Stack", resource_suffix, env=AWS_ENV)

mwaa_stack = MwaaStack(app, "MwaaStack", vpc_stack.mwaa_vpc, s3_stack.s3_bucket, resource_suffix, env=AWS_ENV)

mwaa_stack.add_dependency(s3_stack)
mwaa_stack.add_dependency(vpc_stack)
ltv_stack.add_dependency(vpc_stack)

app.synth()
