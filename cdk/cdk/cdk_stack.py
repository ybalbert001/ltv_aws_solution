from constructs import Construct
from aws_cdk import (
    Duration,
    Stack,
    aws_ec2,
    aws_s3 as s3,
    aws_dynamodb as ddb,
    aws_kinesis as kds,
    aws_redshift as redshift,
    aws_iam as iam
)

import aws_cdk as cdk
from aws_cdk import aws_s3_deployment as s3deploy

class CdkStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, vpc, resource_suffix, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        target_ddb_name = "ltv-poc-ddb-{}".format(resource_suffix)
        target_kinesis_name = "ltv-poc-kinesis-data-stream-{}".format(resource_suffix)

        KINESIS_STREAM_NAME = cdk.CfnParameter(self, 'CreateKinesisStreamNameParam',
          type='String',
          description='kinesis data stream name',
          default=target_kinesis_name
        )
        
        ddb_table = ddb.Table(self, "CreateDynamoDbTable",
            table_name=target_ddb_name,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            partition_key=ddb.Attribute(name="id",
            type=ddb.AttributeType.STRING),
            sort_key=ddb.Attribute(name="order_time",
            type=ddb.AttributeType.STRING),
            time_to_live_attribute="ttl",
            billing_mode=ddb.BillingMode.PROVISIONED,
            read_capacity=15,
            write_capacity=5
        )

        source_kinesis_stream = kds.Stream(self, "CreateSourceKinesisStreams",
            stream_mode=kds.StreamMode.ON_DEMAND,
            stream_name=KINESIS_STREAM_NAME.value_as_string)

        read_from_kds_policy = iam.PolicyDocument()
        read_from_kds_policy.add_statements(iam.PolicyStatement(**{
            "effect": iam.Effect.ALLOW,
            "resources": [ f'arn:aws:kinesis:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:stream/*'],
            "actions": [ 
                "kinesis:DescribeStreamSummary",
                "kinesis:GetShardIterator",
                "kinesis:GetRecords",
                "kinesis:DescribeStream"
            ]
        }))

        read_from_kds_policy.add_statements(iam.PolicyStatement(**{
            "effect": iam.Effect.ALLOW,
            "resources": [ "*" ],
            "actions": [ 
                "kinesis:ListStreams",
                "kinesis:ListShards"
            ]
        }))

        redshift_ml_execution_role = iam.Role(self, 'CreateRedshiftExecutionRole',
            role_name='RedshiftExecutionRole-{}'.format(resource_suffix),
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("sagemaker.amazonaws.com"),
                iam.ServicePrincipal("redshift.amazonaws.com"),
            ),
            inline_policies={
                'read_from_kds_policy': read_from_kds_policy
            },
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name('AmazonS3FullAccess'),
                iam.ManagedPolicy.from_aws_managed_policy_name('AmazonAthenaFullAccess'),
                iam.ManagedPolicy.from_aws_managed_policy_name('AWSGlueConsoleFullAccess'),
                iam.ManagedPolicy.from_aws_managed_policy_name('AmazonSageMakerFullAccess'),
                iam.ManagedPolicy.from_aws_managed_policy_name('AmazonRedshiftAllCommandsFullAccess'),
                iam.ManagedPolicy.from_aws_managed_policy_name('AmazonRedshiftFullAccess')
            ]
        )

        sg_rs_client = aws_ec2.SecurityGroup(self, 'RedshiftClientSG',
            vpc=vpc,
            allow_all_outbound=True,
            description='security group for redshift client',
            security_group_name='redshift-client-sg-{}'.format(resource_suffix)
        )

        sg_rs_cluster = aws_ec2.SecurityGroup(self, 'RedshiftClusterSG',
            vpc=vpc,
            allow_all_outbound=True,
            description='security group for redshift cluster nodes',
            security_group_name='redshift-cluster-sg-{}'.format(resource_suffix)
        )

        sg_rs_cluster.add_ingress_rule(peer=aws_ec2.Peer.any_ipv4(), connection=aws_ec2.Port.tcp(5439), description='for mwaa access',);
        sg_rs_cluster.add_ingress_rule(peer=sg_rs_client, connection=aws_ec2.Port.tcp(5439), description='redshift-client-sg')
        sg_rs_cluster.add_ingress_rule(peer=sg_rs_cluster, connection=aws_ec2.Port.all_tcp(), description='redshift-cluster-sg')
        cdk.Tags.of(sg_rs_cluster).add('Name', 'redshift-cluster-sg')

        # public_subnets_list = vpc.public_subnets
        # # sg_vpc = vpc.vpc_default_security_group
        # private_subnets = vpc.select_subnets(
        #     subnet_type=aws_ec2.SubnetType.PRIVATE_WITH_NAT
        # )

        cfn_redshift_cluster = redshift.CfnCluster(self, "RedshiftCfnCluster",
            cluster_type="multi-node", # [single-node, multi-node]
            db_name="dev",
            master_username="workshop-user",
            master_user_password="workshop-PWD-123",
            node_type="ra3.xlplus",
            number_of_nodes=2,
            cluster_identifier="redshift-001",
            preferred_maintenance_window="sun:03:00-sun:04:00",
            publicly_accessible=False,
            iam_roles=[redshift_ml_execution_role.role_arn],
            vpc_security_group_ids=[sg_rs_cluster.security_group_id]
        )

        cfn_redshift_cluster.node.add_dependency(sg_rs_cluster)
