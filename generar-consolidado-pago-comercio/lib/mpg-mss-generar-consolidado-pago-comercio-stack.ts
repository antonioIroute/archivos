import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as glue from 'aws-cdk-lib/aws-glue';
import { AppConfig } from '../config/BuildConfiguration';

export interface MpgMssGenerarConsolidadoPagoComercioStackProps extends cdk.StackProps {
  config: AppConfig;
}

export class MpgMssGenerarConsolidadoPagoComercioStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: MpgMssGenerarConsolidadoPagoComercioStackProps) {
    super(scope, id, props);

    // *** Valores directamente de props ***
    const { config } = props;
    const stage = config.stage;
    const bucketGlueName = config.bucketGlueName;
    const stackName = config.stackName;
    const subnetId = config.subnetId;
    const securityGroupIdList = config.securityGroupIdList
    const cronSchedule = config.CRON;
    const jdbdConnectionUrl = config.JDBC_CONNECTION_URL;
    const secretId = config.SECRET_ID;

    // Definición del rol para Glue (sin cambios)
    const glueRole = new iam.Role(this, 'GlueServiceConsolidado', {
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonRDSDataFullAccess'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonS3FullAccess')
      ],
    });

    glueRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ["sns:Publish"],
      resources: ["arn:aws:sns:us-east-1:981631255281:alertasBatch"]
    }));

    glueRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ["s3:GetObject"],
      resources: [
        `arn:aws:s3:::bb-adquirenciamdp-config-${stage}/*`,
        `arn:aws:s3:::bb-adquirenciamdp-datasource-${stage}/*`,
        `arn:aws:s3:::bb-adquirenciamdp-datastore-${stage}/*`,
        `arn:aws:s3:::bb-adquirenciamdp-archivosfinales-${stage}/*`,
        `arn:aws:s3:::bb-adquirenciamdp-fuentes-${stage}/*`
      ]
    }));

    glueRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ["s3:PutObject"],
      resources: [
        `arn:aws:s3:::bb-adquirenciamdp-datastore-${stage}/*`,
        `arn:aws:s3:::bb-adquirenciamdp-archivosfinales-${stage}/*`,
        `arn:aws:s3:::bb-adquirenciamdp-logs-${stage}/*`,
        `arn:aws:s3:::bb-adquirenciamdp-datasource-${stage}/*`
      ]
    }));

    glueRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ["secretsmanager:GetSecretValue"],
      resources: [secretId]
    }));

    glueRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        "ssm:GetParameter",
        "ssm:PutParameter",
        "ssm:DeleteParameter",
        "ssm:DescribeParameters"
      ],
      resources: [
        // ? A queé se refiere conque no puede quedar asi ? 
        // *** Para mi este es el arn que debe ir definido para capturar los params
        // ! INVESITGAR o PREGUNTAR
        `arn:aws:ssm:us-east-1:981631255281:parameter/mpg-mss-generar-consolidado/${stage}` 
      ]
    }));

    // Conexión Glue (sin cambios)
    const glueConnectionRDS = new glue.CfnConnection(this, `${stackName}-connection-rds`, {
      catalogId: cdk.Fn.ref('AWS::AccountId'),
      connectionInput: {
        name: `${stackName}-connection-rds`,
        connectionType: 'JDBC',
        connectionProperties: {
          JDBC_ENFORCE_SSL: "false",
          JDBC_CONNECTION_URL: jdbdConnectionUrl,
          SECRET_ID: secretId,
        },
        physicalConnectionRequirements: {
          subnetId: subnetId,
          securityGroupIdList: securityGroupIdList,
          availabilityZone: 'us-east-1a'
        }
      }
    });

    // Trigger sin la condición de START_ON_CREATION
    const glueDailyTrigger = new glue.CfnTrigger(this, `${stackName}-daily-trigger`, {
      name: `${stackName}-daily-trigger`,
      type: 'SCHEDULED',
      schedule: cronSchedule,
      actions: [
        {
          jobName: `${stackName}-bb-inf`
        }
      ],
      startOnCreation: true // activado por defecto
    });

    // Glue job (sin cambios)
    const cfnJob = new glue.CfnJob(this, `${stackName}-bb-inf`, {
      connections: {
        connections: [glueConnectionRDS.ref]
      },
      command: {
        name: 'glueetl',
        pythonVersion: '3',
        scriptLocation: `${bucketGlueName}/job_consolidado_bb.py`,
      },
      role: glueRole.roleArn,
      defaultArguments: {
        "--aws_region": "us-east-1",
        "--env": "bb",
        "--job-bookmark-option": "job-bookmark-disable",
        "--enable-metrics": "true",
        "--enable-continuous-cloudwatch-log": "true",
        "--extra-py-files": `${bucketGlueName}/utils.zip`,
        "--TempDir": `s3://bb-adquirenciamdp-logs-${stage}/temp/`
      },
      executionClass: 'STANDARD',
      executionProperty: {
        maxConcurrentRuns: 5,
      },
      glueVersion: '4.0',
      maxRetries: 0,
      name: `${stackName}-bb-inf`,
      nonOverridableArguments: {},
      timeout: 30,
      workerType: 'G.2X',
      numberOfWorkers: 2
    });

    new cdk.CfnOutput(this, 'ExtractedCron', {
      value: cronSchedule,
      description: 'Extracted CRON value from JSON parameter'
    });
  }
}
