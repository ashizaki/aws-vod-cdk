import { CloudFrontToS3 } from "@aws-solutions-constructs/aws-cloudfront-s3"
import { LambdaToSns } from "@aws-solutions-constructs/aws-lambda-sns"
import { Aws, Stack, StackProps } from "aws-cdk-lib"
import { EventField, Rule, RuleTargetInput } from "aws-cdk-lib/aws-events"
import { LambdaFunction } from "aws-cdk-lib/aws-events-targets"
import { Policy, PolicyStatement, Role, ServicePrincipal } from "aws-cdk-lib/aws-iam"
import { Runtime } from "aws-cdk-lib/aws-lambda"
import { NodejsFunction } from "aws-cdk-lib/aws-lambda-nodejs"
import { BlockPublicAccess, Bucket, BucketEncryption, HttpMethods } from "aws-cdk-lib/aws-s3"
import { Topic } from "aws-cdk-lib/aws-sns"
import { EmailSubscription } from "aws-cdk-lib/aws-sns-subscriptions"

import { Construct } from "constructs"
import * as path from "path"

const SUPPORT_MOVIE_SUFFIX = [
  ".mpg",
  ".mp4",
  ".m4v",
  ".mov",
  ".m2ts",
  ".wmv",
  ".mxf",
  ".mkv",
  ".m3u8",
  ".mpeg",
  ".webm",
  ".h264",
]

interface AwsVodCdkStackProps extends StackProps {
  mediaConvertEndpoint: string
  adminEmail: string
}

export class AwsVodCdkStack extends Stack {
  constructor(scope: Construct, id: string, props: AwsVodCdkStackProps) {
    super(scope, id, props)

    const notificationTopic = new Topic(this, "Topic", {
      topicName: `${this.stackName}-Topic`,
    })

    notificationTopic.addSubscription(new EmailSubscription(props.adminEmail))

    /**
     * Logs bucket for S3 and CloudFront
     */
    const logsBucket = new Bucket(this, "Logs", {
      encryption: BucketEncryption.S3_MANAGED,
      publicReadAccess: false,
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
    })

    /**
     * Source S3 bucket to host source videos and jobSettings JSON files
     */
    const source = new Bucket(this, "Source", {
      serverAccessLogsBucket: logsBucket,
      serverAccessLogsPrefix: "source-bucket-logs/",
      encryption: BucketEncryption.S3_MANAGED,
      publicReadAccess: false,
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      eventBridgeEnabled: true,
    })

    /**
     * Destination S3 bucket to host the mediaconvert outputs
     */
    const destination = new Bucket(this, "Destination", {
      serverAccessLogsBucket: logsBucket,
      serverAccessLogsPrefix: "destination-bucket-logs/",
      encryption: BucketEncryption.S3_MANAGED,
      publicReadAccess: false,
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      cors: [
        {
          maxAge: 3000,
          allowedOrigins: ["*"],
          allowedHeaders: ["*"],
          allowedMethods: [HttpMethods.GET],
        },
      ],
    })

    /**
     * Solutions construct to create Cloudfront with a s3 bucket as the origin
     * https://docs.aws.amazon.com/solutions/latest/constructs/aws-cloudfront-s3.html
     * insertHttpSecurityHeaders is set to false as this requires the deployment to be in us-east-1
     */
    const cloudFront = new CloudFrontToS3(this, "CloudFront", {
      existingBucketObj: destination,
      insertHttpSecurityHeaders: false,
      cloudFrontDistributionProps: {
        defaultCacheBehavior: {
          allowedMethods: ["GET", "HEAD", "OPTIONS"],
          Compress: false,
          forwardedValues: {
            queryString: false,
            headers: ["Origin", "Access-Control-Request-Method", "Access-Control-Request-Headers"],
            cookies: { forward: "none" },
          },
          viewerProtocolPolicy: "allow-all",
        },
        loggingConfig: {
          bucket: logsBucket,
          prefix: "cloudfront-logs",
        },
      },
    })

    /**
     * MediaConvert Service Role to grant Mediaconvert Access to the source and Destination Bucket,
     * API invoke * is also required for the services.
     */
    const mediaConvertRole = new Role(this, "MediaConvertRole", {
      assumedBy: new ServicePrincipal("mediaconvert.amazonaws.com"),
    })

    const mediaConvertPolicy = new Policy(this, "MediaConvertPolicy", {
      statements: [
        new PolicyStatement({
          resources: [`${source.bucketArn}/*`, `${destination.bucketArn}/*`],
          actions: ["s3:GetObject", "s3:PutObject"],
        }),
        new PolicyStatement({
          resources: [`arn:${Aws.PARTITION}:execute-api:${Aws.REGION}:${Aws.ACCOUNT_ID}:*`],
          actions: ["execute-api:Invoke"],
        }),
      ],
    })
    mediaConvertPolicy.attachToRole(mediaConvertRole)

    const jobSubmit = new NodejsFunction(this, "jobSubmit", {
      entry: path.join(__dirname, "job-submit-fn.ts"),
      handler: "handler",
      runtime: Runtime.NODEJS_16_X,
      environment: {
        MEDIACONVERT_ENDPOINT: props.mediaConvertEndpoint,
        MEDIACONVERT_ROLE: mediaConvertRole.roleArn,
        DESTINATION_BUCKET: destination.bucketName,
        STACKNAME: Aws.STACK_NAME,
        SNS_TOPIC_ARN: notificationTopic.topicArn,
      },
      initialPolicy: [
        new PolicyStatement({
          actions: ["iam:PassRole"],
          resources: [mediaConvertRole.roleArn],
        }),
        new PolicyStatement({
          actions: ["mediaconvert:CreateJob"],
          resources: [`arn:${Aws.PARTITION}:mediaconvert:${Aws.REGION}:${Aws.ACCOUNT_ID}:*`],
        }),
        new PolicyStatement({
          actions: ["s3:GetObject"],
          resources: [source.bucketArn, `${source.bucketArn}/*`],
        }),
      ],
    })

    // EventBridge Rule作成
    new Rule(this, `JobSubmitRule`, {
      ruleName: `${this.stackName}-JobSubmitRule`,
      eventPattern: {
        source: ["aws.s3"],
        detailType: ["Object Created"],
        detail: {
          object: {
            key: SUPPORT_MOVIE_SUFFIX.flatMap((val) => {
              return [
                {
                  suffix: val,
                },
                {
                  suffix: val.toUpperCase(),
                },
              ]
            }),
          },
          bucket: {
            name: [source.bucketName],
          },
        },
        resources: [source.bucketArn],
      },
      targets: [
        new LambdaFunction(jobSubmit, {
          event: RuleTargetInput.fromObject({
            id: EventField.eventId,
            account: EventField.account,
            time: EventField.time,
            region: EventField.region,
            "detail-type": EventField.detailType,
            detail: {
              key: EventField.fromPath("$.detail.object.key"),
              size: EventField.fromPath("$.detail.object.size"),
              bucketName: EventField.fromPath("$.detail.bucket.name"),
            },
          }),
        }),
      ],
    })

    const jobComplete = new NodejsFunction(this, "jobComplete", {
      entry: path.join(__dirname, "job-complete-fn.ts"),
      handler: "handler",
      runtime: Runtime.NODEJS_16_X,
      environment: {
        MEDIACONVERT_ENDPOINT: props.mediaConvertEndpoint,
        CLOUDFRONT_DOMAIN: cloudFront.cloudFrontWebDistribution.distributionDomainName,
        SOURCE_BUCKET: source.bucketName,
        STACKNAME: Aws.STACK_NAME,
        SNS_TOPIC_ARN: notificationTopic.topicArn,
      },
      initialPolicy: [
        new PolicyStatement({
          actions: ["mediaconvert:GetJob"],
          resources: [`arn:${Aws.PARTITION}:mediaconvert:${Aws.REGION}:${Aws.ACCOUNT_ID}:*`],
        }),
        new PolicyStatement({
          actions: ["s3:GetObject", "s3:PutObject"],
          resources: [`${source.bucketArn}/*`],
        }),
      ],
    })

    new Rule(this, `jobCompleteRule`, {
      ruleName: `${this.stackName}-jobComplete`,
      eventPattern: {
        source: ["aws.mediaconvert"],
        detailType: ["MediaConvert Job State Change"],
        detail: {
          userMetadata: {
            stackName: [Aws.STACK_NAME],
          },
          status: ["COMPLETE", "ERROR", "CANCELED", "INPUT_INFORMATION"],
        },
      },
      targets: [
        new LambdaFunction(jobComplete, {
          event: RuleTargetInput.fromObject({
            id: EventField.eventId,
            account: EventField.account,
            time: EventField.time,
            region: EventField.region,
            "detail-type": EventField.detailType,
            detail: {
              status: EventField.fromPath("$.detail.status"),
              jobId: EventField.fromPath("$.detail.jobId"),
              outputGroupDetails: EventField.fromPath("$.detail.outputGroupDetails"),
              userMetadata: {
                stackName: EventField.fromPath("$.detail.userMetadata.stackName"),
              },
            },
          }),
        }),
      ],
    })

    new LambdaToSns(this, "Notification", {
      // NOSONAR
      existingLambdaObj: jobSubmit,
      existingTopicObj: notificationTopic,
    })

    new LambdaToSns(this, "CompleteSNS", {
      // NOSONAR
      existingLambdaObj: jobComplete,
      existingTopicObj: notificationTopic,
    })
  }
}
