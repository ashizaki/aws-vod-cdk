import { EventBridgeHandler } from "aws-lambda"
import AWS, { MediaConvert } from "aws-sdk"
import { v4 as uuid } from "uuid"

interface Detail {
  status: string
  jobId: string
  outputGroupDetails: any[]
}

export const handler: EventBridgeHandler<string, Detail, any> = async (event, context) => {
  console.log(`REQUEST:: ${JSON.stringify(event, null, 2)}`)
  const { MEDIACONVERT_ENDPOINT, CLOUDFRONT_DOMAIN, SNS_TOPIC_ARN, STACKNAME } = process.env

  if (!MEDIACONVERT_ENDPOINT || !CLOUDFRONT_DOMAIN || !STACKNAME || !SNS_TOPIC_ARN) {
    throw new Error("Invalid environment values")
  }

  try {
    const status = event.detail.status
    switch (status) {
      case "INPUT_INFORMATION":
        break
      case "COMPLETE":
        const jobDetails = await processJobDetails(
          MEDIACONVERT_ENDPOINT || "",
          CLOUDFRONT_DOMAIN || "",
          event.detail,
        )
        await sendSns(SNS_TOPIC_ARN, STACKNAME, status, jobDetails)
        break
      case "CANCELED":
      case "ERROR":
        /**
         * Send error to SNS
         */
        try {
          await sendSns(SNS_TOPIC_ARN, STACKNAME, status, event)
        } catch (err) {
          throw err
        }
        break
      default:
        throw new Error("Unknown job status")
    }
  } catch (err) {
    await sendSns(SNS_TOPIC_ARN || "", STACKNAME || "", "PROCESSING ERROR", err)
    throw err
  }
  return
}

/**
 * Ge the Job details from MediaConvert and process the MediaConvert output details
 * from Cloudwatch
 */
const processJobDetails = async (endpoint: string, cloudfrontUrl: string, detail: Detail) => {
  console.log("Processing MediaConvert outputs")
  const buildUrl = (originalValue: string) => originalValue.slice(5).split("/").splice(1).join("/")
  const mediaconvert = new AWS.MediaConvert({
    endpoint: endpoint,
    customUserAgent: process.env.SOLUTION_IDENTIFIER,
  })

  try {
    const jobData = await mediaconvert.getJob({ Id: detail.jobId }).promise()
    const jobDetails = {
      id: detail.jobId,
      job: jobData.Job,
      outputGroupDetails: detail.outputGroupDetails,
      playlistFile: detail.outputGroupDetails.map(
        (output) => `https://${cloudfrontUrl}/${buildUrl(output.playlistFilePaths[0])}`,
      ),
    }
    console.log(`JOB DETAILS:: ${JSON.stringify(jobDetails, null, 2)}`)
    return jobDetails
  } catch (err) {
    console.error(err)
    throw err
  }
}

/**
 * Send An sns notification for any failed jobs
 */
const sendSns = async (topic: string, stackName: string, status: string, data: any) => {
  const sns = new AWS.SNS({
    region: process.env.REGION,
  })
  try {
    let id, msg

    switch (status) {
      case "COMPLETE":
        /**
         * reduce the data object just send Id,InputFile, Outputs
         */
        id = data.Id
        msg = {
          Id: data.Id,
          InputFile: data.InputFile,
          InputDetails: data.InputDetails,
          Outputs: data.Outputs,
        }
        break
      case "CANCELED":
      case "ERROR":
        /**
         * Adding CloudWatch log link for failed jobs
         */
        id = data.detail.jobId
        msg = {
          Details: `https://console.aws.amazon.com/mediaconvert/home?region=${process.env.AWS_REGION}#/jobs/summary/${id}`,
          ErrorMsg: data,
        }
        break
      case "PROCESSING ERROR":
        /**
         * Edge case where processing the MediaConvert outputs fails.
         */
        id = data.Job.detail.jobId || data.detail.jobId
        msg = data
        break
    }
    console.log(`Sending ${status} SNS notification ${id}`)
    await sns
      .publish({
        TargetArn: topic,
        Message: JSON.stringify(msg, null, 2),
        Subject: `${stackName}: Job ${status} id:${id}`,
      })
      .promise()
  } catch (err) {
    console.error(err)
    throw err
  }
}
