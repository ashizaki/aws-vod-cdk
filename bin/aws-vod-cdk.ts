#!/usr/bin/env node
import "source-map-support/register"
import * as cdk from "aws-cdk-lib"
import { MediaConvert } from "aws-sdk"
import { AwsVodCdkStack } from "lib/aws-vod-cdk-stack"

export const getMediaConvertEndpoint = async () => {
  const mediaConvert = new MediaConvert({ apiVersion: "2017-08-29" })
  const ret = await mediaConvert.describeEndpoints({ MaxResults: 0 }).promise()
  if (!!ret.Endpoints && ret.Endpoints.length > 0) {
    return ret.Endpoints[0].Url
  }
  return ""
}

const app = new cdk.App()
const adminEmail = app.node.tryGetContext("admin-email")
if (!adminEmail) {
  console.log("--context admin-email=xxxx@xxxx.xx is required")
} else {
  async function main() {
    const endpoint = await getMediaConvertEndpoint()
    new AwsVodCdkStack(app, "AwsVodCdkStack", {
      mediaConvertEndpoint: endpoint || "",
      adminEmail: adminEmail,
    })
  }

  main()
}
