import { CfnResource, Stack, StackProps } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as cdk from 'aws-cdk-lib';
import { Asset } from 'aws-cdk-lib/aws-s3-assets';
import * as rds from 'aws-cdk-lib/aws-rds';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as ecs from 'aws-cdk-lib/aws-ecs';
import * as kinesis from 'aws-cdk-lib/aws-kinesis';
import { aws_kinesisfirehose as kinesisfirehose } from 'aws-cdk-lib';
import { aws_dms as dms } from 'aws-cdk-lib';
import * as iam from "aws-cdk-lib/aws-iam";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as path from 'path';
import { aws_glue as glue } from 'aws-cdk-lib';

export class FinTransactionsStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const region = cdk.Stack.of(this).region
    const dbUserName = 'clusteradmin'


    // Create an S3 bucket where source and intermediate data is retained
    //1. Setup necesaary resources to be used with our tasks
    const resourcesObject = new Asset(this, "AccountData", {
      path: path.join(__dirname, "../resources/data")
    });

    // create an Aurora MySQL Database cluster for receiving transactions
    const vpc = new ec2.Vpc(this, "databaseVpc");
    // we get the security group of the above VPC and use the same for task clusters 
    const securityGroup = ec2.SecurityGroup.fromSecurityGroupId(this, "secGroup", vpc.vpcDefaultSecurityGroup)
    securityGroup.addIngressRule(ec2.Peer.securityGroupId(securityGroup.securityGroupId),ec2.Port.tcp(3306),"MySQL Ingress")

    // create a MySQL parameter group
    const parameterGroup = new rds.ParameterGroup(this, 'MySQLAuroraParameterGroup', {
      engine: rds.DatabaseClusterEngine.auroraMysql({ version: rds.AuroraMysqlEngineVersion.VER_2_08_1 }),
      parameters: {
        binlog_format: 'ROW',
        binlog_row_image: 'full',
        binlog_rows_query_log_events: 'ON'
      },
    });

    const dbCluster = new rds.DatabaseCluster(this, 'Database', {
      engine: rds.DatabaseClusterEngine.auroraMysql({ version: rds.AuroraMysqlEngineVersion.VER_2_08_1 }),
      credentials: rds.Credentials.fromGeneratedSecret(dbUserName),
      instanceProps: { 
        instanceType: ec2.InstanceType.of(ec2.InstanceClass.R5, ec2.InstanceSize.LARGE),
        vpcSubnets: {
          subnetType: ec2.SubnetType.PRIVATE,
        },
        vpc,
        securityGroups:[securityGroup],
      },
      parameterGroup:parameterGroup,
      s3ImportBuckets: [resourcesObject.bucket],
      defaultDatabaseName:"workshopDb",
    });

    const proxy = new rds.DatabaseProxy(this, 'Proxy', {
      proxyTarget: rds.ProxyTarget.fromCluster(dbCluster),
      secrets: [dbCluster.secret!],
      vpc,
      securityGroups:[securityGroup],
      iamAuth:true
    });


    // create a farget task to initiatize data and send test trasactions into the database
    // Create an ECS cluster
    const ecsCluster = new ecs.Cluster(this, 'Cluster', {
      vpc,
      enableFargateCapacityProviders: true,
    });
    // Add CloudWatch Logging so we can monitor console logs during training
    const datagenLogging = new ecs.AwsLogDriver({
        streamPrefix: id + "log",
    });

    const generateDatataskDefinition = new ecs.FargateTaskDefinition(this, 'generateDataFargateTask');
    resourcesObject.bucket.grantRead(generateDatataskDefinition.taskRole);
    proxy.grantConnect(generateDatataskDefinition.taskRole, dbUserName);

    generateDatataskDefinition.addContainer('generateDataContainer', {
      image: ecs.ContainerImage.fromRegistry('amazon/aws-sam-cli-build-image-python3.8'),
      memoryLimitMiB: 512,
      command: [
        "aws s3 cp $DATA_FILES project.zip && unzip project.zip && pip install -r requirements.txt && python3 setupTables.py -u $USER_NAME -e $DATABASE_ENDPOINT && python3 generatedata.py && ls -al account_ids.txt && python3 updateTables.py -u $USER_NAME -e $DATABASE_ENDPOINT",
      ],
      entryPoint: ["sh","-c"], 
      logging:datagenLogging,
      environment:{
        "DATA_FILES":resourcesObject.s3ObjectUrl,
        "DATA_BUCKET":resourcesObject.s3BucketName,
        "DATABASE_ENDPOINT":proxy.endpoint,
        "USER_NAME":dbUserName,
      }
    });
    
    // ingestion stack 

    // create a kinesis sgtream as target for the RDS data.
    const stream = new kinesis.Stream(this, 'TransactionDataStream', {
      streamName: 'fin-transactions-stream',
    });

    // setup a role to access the Database secret from dms
    const dmsServiceAccessRole = new iam.Role(this,"dbSecretAccessRole",{
      assumedBy: new iam.ServicePrincipal(`dms.${region}.amazonaws.com`),
    });
    dbCluster.secret?.grantRead(dmsServiceAccessRole);
    stream.grantReadWrite(dmsServiceAccessRole);
    
    
    // create the source endpoint for reading the Aurora Database
    const sourceEndpoint = new dms.CfnEndpoint(this, 'AuroraSourceEndpoint', {
      endpointType: 'source',
      engineName: 'aurora',
      // the properties below are optional
      databaseName: 'workshopDb',
      mySqlSettings: {
          secretsManagerAccessRoleArn: dmsServiceAccessRole.roleArn,
          secretsManagerSecretId: dbCluster.secret!.secretArn,
      },
    });

    // create the target endpoint for reading the Aurora Database
    const targetEndpoint = new dms.CfnEndpoint(this, 'KinesisTargetEndpoint', {
      endpointType: 'target',
      engineName: 'kinesis',
      // the properties below are optional
      kinesisSettings: {
        messageFormat: 'JSON',
        noHexPrefix: false,
        partitionIncludeSchemaTable: false,
        serviceAccessRoleArn: dmsServiceAccessRole.roleArn,
        streamArn: stream.streamArn,
      },
    });

    
    // create the replication instance and task
    const txnReplicationSubnetGroup = new dms.CfnReplicationSubnetGroup(this, 'TxnAnlyticsReplicationSubnetGroup', {
      replicationSubnetGroupDescription: 'TxnAnlyticsReplicationSubnetGroup',
      subnetIds: [
        vpc.privateSubnets[0].subnetId,
        vpc.privateSubnets[1].subnetId,
        // vpc.publicSubnets[0].subnetId, // allow only private subnets since the DB is running in private subnet
        // vpc.publicSubnets[1].subnetId
      ],
    });
    
    const replicationInstance = new dms.CfnReplicationInstance(this, 'TransAnalyticsReplicationInstance', {
      replicationInstanceClass: 'dms.c5.large',
      replicationSubnetGroupIdentifier:txnReplicationSubnetGroup.ref,
      vpcSecurityGroupIds:[securityGroup.securityGroupId]
    });
    
    const replicationTask = new dms.CfnReplicationTask(this, 'TransAnalyticsReplicationTask', {
      migrationType: 'full-load-and-cdc',
      replicationInstanceArn: replicationInstance.ref, // "arn:aws:dms:ap-south-1:332009426877:rep:U3STAZJJO7SYJWN7YVVKR3ZPNPWZ5S74ZHEPSRA", //replicationInstance.ref,
      sourceEndpointArn: sourceEndpoint.ref,
      tableMappings: '{"rules": [{"rule-type": "selection","rule-id": "281805402","rule-name": "281805402","object-locator": {"schema-name": "workshopDb","table-name": "%"},"rule-action": "include","filters": []}]}',
      targetEndpointArn: targetEndpoint.ref,
    });

    // consumption stack
    
    const kinesisServiceAccessRole = new iam.Role(this,"kinesisServiceAccessRole",{
      assumedBy: new iam.ServicePrincipal("firehose.amazonaws.com"),
    });

    const firehosePolicy = new iam.Policy(this, 'FirehosePolicy', {
        roles: [kinesisServiceAccessRole],
        statements: [
            new iam.PolicyStatement({
                effect: iam.Effect.ALLOW,
                resources: [stream.streamArn],
                actions: ['kinesis:DescribeStream', 'kinesis:GetShardIterator', 'kinesis:GetRecords'],
            }),
        ],
    });
    stream.grantRead(kinesisServiceAccessRole);

    // create a target s3 bucket.
    const targetBucket = new s3.Bucket(this, "targetDataBucket");
    targetBucket.grantReadWrite(kinesisServiceAccessRole);
    

    // create a kinesis data firehose delivery stream to move the data to S3
    const firehoseDeliveryStream = new kinesisfirehose.CfnDeliveryStream(this, 'FinTransactionsDeliveryStream',
      {
        deliveryStreamName:"FinTransactionsDeliveryStream",
        deliveryStreamType:"KinesisStreamAsSource",
        s3DestinationConfiguration:{
          bucketArn: targetBucket.bucketArn,
          roleArn: kinesisServiceAccessRole.roleArn,
        },
        kinesisStreamSourceConfiguration:{
          kinesisStreamArn: stream.streamArn,
          roleArn: kinesisServiceAccessRole.roleArn,
        }
      }
    );
    firehoseDeliveryStream.addDependsOn(firehosePolicy.node.defaultChild as CfnResource);

    // stack outputs
    new cdk.CfnOutput(
      this, 
      'generateDataCommand', 
      { value: `aws ecs run-task --cluster ${ecsCluster.clusterName} --capacity-provider-strategy capacityProvider=FARGATE,base=0,weight=1 --network-configuration "awsvpcConfiguration={subnets=[${vpc.publicSubnets[0].subnetId}],assignPublicIp='ENABLED'}" --task-definition ${generateDatataskDefinition.family}` }
    );

    new cdk.CfnOutput(
      this, 
      'startReplicationTask', 
      { value: `aws dms start-replication-task --replication-task-arn ${replicationTask.ref} --start-replication-task-type start-replication` }
    );
    

  }
}
