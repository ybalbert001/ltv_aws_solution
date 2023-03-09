#!/usr/bin/env python3

import aws_cdk as cdk

from aws_cdk import (
    Stack,
    aws_ec2,
)
from constructs import Construct

class VpcStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        #XXX: For creating this CDK Stack in the existing VPC,
        # remove comments from the below codes and
        # comments out vpc = aws_ec2.Vpc(..) codes,
        # then pass -c vpc_name=your-existing-vpc to cdk command
        # for example,
        # cdk -c vpc_name=your-existing-vpc syth
        #
        # vpc_name = self.node.try_get_context('vpc_name')
        # vpc = aws_ec2.Vpc.from_lookup(self, 'ExistingVPC',
        #   is_default=True,
        #   vpc_name=vpc_name
        # )

        #XXX: To use more than 2 AZs, be sure to specify the account and region on your stack.
        #XXX: https://docs.aws.amazon.com/cdk/api/latest/python/aws_cdk.aws_ec2/Vpc.html
        mwaa_vpc = aws_ec2.Vpc(self, 'MwaaVPC',
          ip_addresses=aws_ec2.IpAddresses.cidr("10.192.0.0/16"),
          max_azs=2,
          subnet_configuration=[
              {
                  "cidrMask": 21,
                  "name": "Public",
                  "subnetType": aws_ec2.SubnetType.PUBLIC,
              },
              {
                  "cidrMask": 21,
                  "name": "Private",
                  "subnetType": aws_ec2.SubnetType.PRIVATE_WITH_EGRESS
              }
          ],
          gateway_endpoints={
            "S3": aws_ec2.GatewayVpcEndpointOptions(
              service=aws_ec2.GatewayVpcEndpointAwsService.S3
            )
          },
          vpc_name="MWAA_VPC"
        )

        vpc = aws_ec2.Vpc.from_lookup(self, "GetDefaultVPC", is_default=True)

        vpc_peering = aws_ec2.CfnVPCPeeringConnection(self, "MyCfnVPCPeeringConnection",
            peer_vpc_id=mwaa_vpc.vpc_id,
            vpc_id=vpc.vpc_id,
        )
        
        default_route_ids = list(set([ subnet.route_table.route_table_id for subnet in vpc.public_subnets ]))
        mwaa_route_ids = list(set([ subnet.route_table.route_table_id for subnet in mwaa_vpc.public_subnets ]))

        route_id = 0
        for routetable_id in default_route_ids:
          route_id += 1
          aws_ec2.CfnRoute(self, 
            "PeeringRouteNumber-{}".format(route_id), 
            destination_cidr_block="10.192.0.0/16",
            route_table_id=routetable_id,
            vpc_peering_connection_id=vpc_peering.ref,
          )

        for routetable_id in mwaa_route_ids:
          route_id += 1
          aws_ec2.CfnRoute(self, 
            "PeeringRouteNumber-{}".format(route_id), 
            destination_cidr_block=vpc.vpc_cidr_block,
            route_table_id=routetable_id,
            vpc_peering_connection_id=vpc_peering.ref,
          )

        self.mwaa_vpc = mwaa_vpc
        self.redshift_vpc = vpc

        cdk.CfnOutput(self, 'Mwaa-VpcId', value=mwaa_vpc.vpc_id, export_name='Mwaa-VpcId')
        cdk.CfnOutput(self, 'Redshift-VpcId', value=vpc.vpc_id, export_name='Redshift-VpcId')
