import { Stack, StackProps } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as cdk from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import { Asset } from 'aws-cdk-lib/aws-s3-assets';
import * as rds from 'aws-cdk-lib/aws-rds';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as ecs from 'aws-cdk-lib/aws-ecs';
import * as path from 'path';

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
      s3ImportBuckets: [resourcesObject.bucket],
      defaultDatabaseName:"workshopDb",
      // iamAuthentication:true
    });

    const proxy = new rds.DatabaseProxy(this, 'Proxy', {
      proxyTarget: rds.ProxyTarget.fromCluster(dbCluster),
      secrets: [dbCluster.secret!],
      vpc,
      securityGroups:[securityGroup],
      iamAuth:true
    });

    const secretJson = dbCluster.secret?.secretValue.toJSON();

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
    

    new cdk.CfnOutput(this, 'generateDataCommand', { value: `aws ecs run-task --cluster ${ecsCluster.clusterName} --capacity-provider-strategy capacityProvider=FARGATE,base=0,weight=1 --network-configuration "awsvpcConfiguration={subnets=[${vpc.publicSubnets[0].subnetId}],assignPublicIp='ENABLED'}" --task-definition ${generateDatataskDefinition.family}` })

  }
}
