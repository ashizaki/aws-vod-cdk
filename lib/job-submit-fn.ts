import { EventBridgeHandler } from "aws-lambda"
import AWS, { MediaConvert } from "aws-sdk"
import { v4 as uuid } from "uuid"

export interface JobSubmitFnParam {
  key: string
  size: string
  bucketName: string
}

export const handler: EventBridgeHandler<string, JobSubmitFnParam, any> = async (
  event,
  context,
) => {
  const { MEDIACONVERT_ENDPOINT, MEDIACONVERT_ROLE, DESTINATION_BUCKET, STACKNAME, SNS_TOPIC_ARN } =
    process.env

  try {
    console.log("EVENT: " + JSON.stringify(event, null, 2))
    const srcVideo = decodeURIComponent(event.detail.key.replace(/\+/g, " "))
    const srcBucket = decodeURIComponent(event.detail.bucketName)
    const guid = uuid()
    const inputPath = `s3://${srcBucket}/${srcVideo}`
    const outputPath = `s3://${DESTINATION_BUCKET}/${guid}`
    const metaData = {
      guid: guid,
      stackName: STACKNAME || "",
    }

    const jobRequest = createJobRequest(inputPath, outputPath, MEDIACONVERT_ROLE || "", metaData)

    const mediaConvert = new MediaConvert({
      apiVersion: "2017-08-29",
      endpoint: MEDIACONVERT_ENDPOINT,
    })

    await mediaConvert.createJob(jobRequest).promise()
    console.log(`job subbmited to MediaConvert:: ${JSON.stringify(jobRequest, null, 2)}`)
  } catch (err) {
    await sendError(SNS_TOPIC_ARN || "", STACKNAME || "", context.logGroupName, err)
  }
  return
}

const createJobRequest = (
  inputPath: string,
  outputPath: string,
  role: string,
  metaData: { guid: string; stackName: string },
) => {
  return {
    Queue: "Default",
    Role: role,
    Settings: {
      OutputGroups: [
        {
          Name: "Apple HLS",
          Outputs: [
            {
              ContainerSettings: {
                Container: "M3U8",
                M3u8Settings: {
                  AudioFramesPerPes: 4,
                  PcrControl: "PCR_EVERY_PES_PACKET",
                  PmtPid: 480,
                  PrivateMetadataPid: 503,
                  ProgramNumber: 1,
                  PatInterval: 0,
                  PmtInterval: 0,
                  VideoPid: 481,
                  AudioPids: [
                    482, 483, 484, 485, 486, 487, 488, 489, 490, 491, 492, 493, 494, 495, 496, 497,
                    498,
                  ],
                },
              },
              VideoDescription: {
                Width: 480,
                ScalingBehavior: "DEFAULT",
                Height: 270,
                TimecodeInsertion: "DISABLED",
                AntiAlias: "ENABLED",
                Sharpness: 100,
                CodecSettings: {
                  Codec: "H_264",
                  H264Settings: {
                    InterlaceMode: "PROGRESSIVE",
                    ParNumerator: 1,
                    NumberReferenceFrames: 3,
                    Syntax: "DEFAULT",
                    GopClosedCadence: 1,
                    HrdBufferInitialFillPercentage: 90,
                    GopSize: 3,
                    Slices: 1,
                    GopBReference: "ENABLED",
                    HrdBufferSize: 1000000,
                    MaxBitrate: 400000,
                    SlowPal: "DISABLED",
                    ParDenominator: 1,
                    SpatialAdaptiveQuantization: "ENABLED",
                    TemporalAdaptiveQuantization: "ENABLED",
                    FlickerAdaptiveQuantization: "ENABLED",
                    EntropyEncoding: "CABAC",
                    RateControlMode: "QVBR",
                    QvbrSettings: {
                      QvbrQualityLevel: 7,
                    },
                    CodecProfile: "HIGH",
                    Telecine: "NONE",
                    MinIInterval: 0,
                    AdaptiveQuantization: "MEDIUM",
                    CodecLevel: "AUTO",
                    FieldEncoding: "PAFF",
                    SceneChangeDetect: "ENABLED",
                    QualityTuningLevel: "SINGLE_PASS_HQ",
                    UnregisteredSeiTimecode: "DISABLED",
                    GopSizeUnits: "SECONDS",
                    ParControl: "SPECIFIED",
                    NumberBFramesBetweenReferenceFrames: 5,
                    RepeatPps: "DISABLED",
                    DynamicSubGop: "ADAPTIVE",
                  },
                },
                AfdSignaling: "NONE",
                DropFrameTimecode: "ENABLED",
                RespondToAfd: "NONE",
                ColorMetadata: "INSERT",
              },
              AudioDescriptions: [
                {
                  AudioTypeControl: "FOLLOW_INPUT",
                  AudioSourceName: "Audio Selector 1",
                  CodecSettings: {
                    Codec: "AAC",
                    AacSettings: {
                      AudioDescriptionBroadcasterMix: "NORMAL",
                      Bitrate: 64000,
                      RateControlMode: "CBR",
                      CodecProfile: "HEV1",
                      CodingMode: "CODING_MODE_2_0",
                      RawFormat: "NONE",
                      SampleRate: 48000,
                      Specification: "MPEG4",
                    },
                  },
                  LanguageCodeControl: "FOLLOW_INPUT",
                  AudioType: 0,
                },
              ],
              NameModifier: "_Ott_Hls_Ts_Avc_Aac_16x9_480x270p_0.4Mbps_qvbr",
            },
            {
              ContainerSettings: {
                Container: "M3U8",
                M3u8Settings: {
                  AudioFramesPerPes: 4,
                  PcrControl: "PCR_EVERY_PES_PACKET",
                  PmtPid: 480,
                  PrivateMetadataPid: 503,
                  ProgramNumber: 1,
                  PatInterval: 0,
                  PmtInterval: 0,
                  VideoPid: 481,
                  AudioPids: [
                    482, 483, 484, 485, 486, 487, 488, 489, 490, 491, 492, 493, 494, 495, 496, 497,
                    498,
                  ],
                },
              },
              VideoDescription: {
                Width: 640,
                ScalingBehavior: "DEFAULT",
                Height: 360,
                TimecodeInsertion: "DISABLED",
                AntiAlias: "ENABLED",
                Sharpness: 100,
                CodecSettings: {
                  Codec: "H_264",
                  H264Settings: {
                    InterlaceMode: "PROGRESSIVE",
                    ParNumerator: 1,
                    NumberReferenceFrames: 3,
                    Syntax: "DEFAULT",
                    GopClosedCadence: 1,
                    HrdBufferInitialFillPercentage: 90,
                    GopSize: 3,
                    Slices: 1,
                    GopBReference: "ENABLED",
                    HrdBufferSize: 3750000,
                    MaxBitrate: 1500000,
                    SlowPal: "DISABLED",
                    ParDenominator: 1,
                    SpatialAdaptiveQuantization: "ENABLED",
                    TemporalAdaptiveQuantization: "ENABLED",
                    FlickerAdaptiveQuantization: "ENABLED",
                    EntropyEncoding: "CABAC",
                    RateControlMode: "QVBR",
                    QvbrSettings: {
                      QvbrQualityLevel: 7,
                    },
                    CodecProfile: "HIGH",
                    Telecine: "NONE",
                    MinIInterval: 0,
                    AdaptiveQuantization: "MEDIUM",
                    CodecLevel: "AUTO",
                    FieldEncoding: "PAFF",
                    SceneChangeDetect: "ENABLED",
                    QualityTuningLevel: "SINGLE_PASS_HQ",
                    UnregisteredSeiTimecode: "DISABLED",
                    GopSizeUnits: "SECONDS",
                    ParControl: "SPECIFIED",
                    NumberBFramesBetweenReferenceFrames: 5,
                    RepeatPps: "DISABLED",
                    DynamicSubGop: "ADAPTIVE",
                  },
                },
                AfdSignaling: "NONE",
                DropFrameTimecode: "ENABLED",
                RespondToAfd: "NONE",
                ColorMetadata: "INSERT",
              },
              AudioDescriptions: [
                {
                  AudioTypeControl: "FOLLOW_INPUT",
                  AudioSourceName: "Audio Selector 1",
                  CodecSettings: {
                    Codec: "AAC",
                    AacSettings: {
                      AudioDescriptionBroadcasterMix: "NORMAL",
                      Bitrate: 64000,
                      RateControlMode: "CBR",
                      CodecProfile: "HEV1",
                      CodingMode: "CODING_MODE_2_0",
                      RawFormat: "NONE",
                      SampleRate: 48000,
                      Specification: "MPEG4",
                    },
                  },
                  LanguageCodeControl: "FOLLOW_INPUT",
                  AudioType: 0,
                },
              ],
              NameModifier: "_Ott_Hls_Ts_Avc_Aac_16x9_640x360p_1.5Mbps_qvbr",
            },
            {
              ContainerSettings: {
                Container: "M3U8",
                M3u8Settings: {
                  AudioFramesPerPes: 4,
                  PcrControl: "PCR_EVERY_PES_PACKET",
                  PmtPid: 480,
                  PrivateMetadataPid: 503,
                  ProgramNumber: 1,
                  PatInterval: 0,
                  PmtInterval: 0,
                  VideoPid: 481,
                  AudioPids: [
                    482, 483, 484, 485, 486, 487, 488, 489, 490, 491, 492, 493, 494, 495, 496, 497,
                    498,
                  ],
                },
              },
              VideoDescription: {
                Width: 960,
                ScalingBehavior: "DEFAULT",
                Height: 540,
                TimecodeInsertion: "DISABLED",
                AntiAlias: "ENABLED",
                Sharpness: 100,
                CodecSettings: {
                  Codec: "H_264",
                  H264Settings: {
                    InterlaceMode: "PROGRESSIVE",
                    ParNumerator: 1,
                    NumberReferenceFrames: 3,
                    Syntax: "DEFAULT",
                    GopClosedCadence: 1,
                    HrdBufferInitialFillPercentage: 90,
                    GopSize: 3,
                    Slices: 1,
                    GopBReference: "ENABLED",
                    HrdBufferSize: 8750000,
                    MaxBitrate: 3500000,
                    SlowPal: "DISABLED",
                    ParDenominator: 1,
                    SpatialAdaptiveQuantization: "ENABLED",
                    TemporalAdaptiveQuantization: "ENABLED",
                    FlickerAdaptiveQuantization: "ENABLED",
                    EntropyEncoding: "CABAC",
                    RateControlMode: "QVBR",
                    QvbrSettings: {
                      QvbrQualityLevel: 8,
                    },
                    CodecProfile: "HIGH",
                    Telecine: "NONE",
                    MinIInterval: 0,
                    AdaptiveQuantization: "HIGH",
                    CodecLevel: "AUTO",
                    FieldEncoding: "PAFF",
                    SceneChangeDetect: "ENABLED",
                    QualityTuningLevel: "SINGLE_PASS_HQ",
                    UnregisteredSeiTimecode: "DISABLED",
                    GopSizeUnits: "SECONDS",
                    ParControl: "SPECIFIED",
                    NumberBFramesBetweenReferenceFrames: 5,
                    RepeatPps: "DISABLED",
                    DynamicSubGop: "ADAPTIVE",
                  },
                },
                AfdSignaling: "NONE",
                DropFrameTimecode: "ENABLED",
                RespondToAfd: "NONE",
                ColorMetadata: "INSERT",
              },
              AudioDescriptions: [
                {
                  AudioTypeControl: "FOLLOW_INPUT",
                  AudioSourceName: "Audio Selector 1",
                  CodecSettings: {
                    Codec: "AAC",
                    AacSettings: {
                      AudioDescriptionBroadcasterMix: "NORMAL",
                      Bitrate: 96000,
                      RateControlMode: "CBR",
                      CodecProfile: "HEV1",
                      CodingMode: "CODING_MODE_2_0",
                      RawFormat: "NONE",
                      SampleRate: 48000,
                      Specification: "MPEG4",
                    },
                  },
                  LanguageCodeControl: "FOLLOW_INPUT",
                  AudioType: 0,
                },
              ],
              NameModifier: "_Ott_Hls_Ts_Avc_Aac_16x9_960x540p_3.5Mbps_qvbr",
            },
            {
              ContainerSettings: {
                Container: "M3U8",
                M3u8Settings: {
                  AudioFramesPerPes: 4,
                  PcrControl: "PCR_EVERY_PES_PACKET",
                  PmtPid: 480,
                  PrivateMetadataPid: 503,
                  ProgramNumber: 1,
                  PatInterval: 0,
                  PmtInterval: 0,
                  VideoPid: 481,
                  AudioPids: [
                    482, 483, 484, 485, 486, 487, 488, 489, 490, 491, 492, 493, 494, 495, 496, 497,
                    498,
                  ],
                },
              },
              VideoDescription: {
                Width: 1280,
                ScalingBehavior: "DEFAULT",
                Height: 720,
                TimecodeInsertion: "DISABLED",
                AntiAlias: "ENABLED",
                Sharpness: 100,
                CodecSettings: {
                  Codec: "H_264",
                  H264Settings: {
                    InterlaceMode: "PROGRESSIVE",
                    ParNumerator: 1,
                    NumberReferenceFrames: 3,
                    Syntax: "DEFAULT",
                    GopClosedCadence: 1,
                    HrdBufferInitialFillPercentage: 90,
                    GopSize: 3,
                    Slices: 1,
                    GopBReference: "ENABLED",
                    HrdBufferSize: 15000000,
                    MaxBitrate: 6000000,
                    SlowPal: "DISABLED",
                    ParDenominator: 1,
                    SpatialAdaptiveQuantization: "ENABLED",
                    TemporalAdaptiveQuantization: "ENABLED",
                    FlickerAdaptiveQuantization: "ENABLED",
                    EntropyEncoding: "CABAC",
                    RateControlMode: "QVBR",
                    QvbrSettings: {
                      QvbrQualityLevel: 8,
                    },
                    CodecProfile: "HIGH",
                    Telecine: "NONE",
                    MinIInterval: 0,
                    AdaptiveQuantization: "HIGH",
                    CodecLevel: "AUTO",
                    FieldEncoding: "PAFF",
                    SceneChangeDetect: "ENABLED",
                    QualityTuningLevel: "SINGLE_PASS_HQ",
                    UnregisteredSeiTimecode: "DISABLED",
                    GopSizeUnits: "SECONDS",
                    ParControl: "SPECIFIED",
                    NumberBFramesBetweenReferenceFrames: 5,
                    RepeatPps: "DISABLED",
                    DynamicSubGop: "ADAPTIVE",
                  },
                },
                AfdSignaling: "NONE",
                DropFrameTimecode: "ENABLED",
                RespondToAfd: "NONE",
                ColorMetadata: "INSERT",
              },
              AudioDescriptions: [
                {
                  AudioTypeControl: "FOLLOW_INPUT",
                  AudioSourceName: "Audio Selector 1",
                  CodecSettings: {
                    Codec: "AAC",
                    AacSettings: {
                      AudioDescriptionBroadcasterMix: "NORMAL",
                      Bitrate: 96000,
                      RateControlMode: "CBR",
                      CodecProfile: "HEV1",
                      CodingMode: "CODING_MODE_2_0",
                      RawFormat: "NONE",
                      SampleRate: 48000,
                      Specification: "MPEG4",
                    },
                  },
                  LanguageCodeControl: "FOLLOW_INPUT",
                  AudioType: 0,
                },
              ],
              NameModifier: "_Ott_Hls_Ts_Avc_Aac_16x9_1280x720p_6.0Mbps_qvbr",
            },
            {
              ContainerSettings: {
                Container: "M3U8",
                M3u8Settings: {
                  AudioFramesPerPes: 4,
                  PcrControl: "PCR_EVERY_PES_PACKET",
                  PmtPid: 480,
                  PrivateMetadataPid: 503,
                  ProgramNumber: 1,
                  PatInterval: 0,
                  PmtInterval: 0,
                  VideoPid: 481,
                  AudioPids: [
                    482, 483, 484, 485, 486, 487, 488, 489, 490, 491, 492, 493, 494, 495, 496, 497,
                    498,
                  ],
                },
              },
              VideoDescription: {
                Width: 1920,
                ScalingBehavior: "DEFAULT",
                Height: 1080,
                TimecodeInsertion: "DISABLED",
                AntiAlias: "ENABLED",
                Sharpness: 100,
                CodecSettings: {
                  Codec: "H_264",
                  H264Settings: {
                    InterlaceMode: "PROGRESSIVE",
                    ParNumerator: 1,
                    NumberReferenceFrames: 3,
                    Syntax: "DEFAULT",
                    GopClosedCadence: 1,
                    HrdBufferInitialFillPercentage: 90,
                    GopSize: 3,
                    Slices: 1,
                    GopBReference: "ENABLED",
                    HrdBufferSize: 21250000,
                    MaxBitrate: 8500000,
                    SlowPal: "DISABLED",
                    ParDenominator: 1,
                    SpatialAdaptiveQuantization: "ENABLED",
                    TemporalAdaptiveQuantization: "ENABLED",
                    FlickerAdaptiveQuantization: "ENABLED",
                    EntropyEncoding: "CABAC",
                    RateControlMode: "QVBR",
                    QvbrSettings: {
                      QvbrQualityLevel: 9,
                    },
                    CodecProfile: "HIGH",
                    Telecine: "NONE",
                    MinIInterval: 0,
                    AdaptiveQuantization: "HIGH",
                    CodecLevel: "AUTO",
                    FieldEncoding: "PAFF",
                    SceneChangeDetect: "ENABLED",
                    QualityTuningLevel: "SINGLE_PASS_HQ",
                    UnregisteredSeiTimecode: "DISABLED",
                    GopSizeUnits: "SECONDS",
                    ParControl: "SPECIFIED",
                    NumberBFramesBetweenReferenceFrames: 5,
                    RepeatPps: "DISABLED",
                    DynamicSubGop: "ADAPTIVE",
                  },
                },
                AfdSignaling: "NONE",
                DropFrameTimecode: "ENABLED",
                RespondToAfd: "NONE",
                ColorMetadata: "INSERT",
              },
              AudioDescriptions: [
                {
                  AudioTypeControl: "FOLLOW_INPUT",
                  AudioSourceName: "Audio Selector 1",
                  CodecSettings: {
                    Codec: "AAC",
                    AacSettings: {
                      AudioDescriptionBroadcasterMix: "NORMAL",
                      Bitrate: 128000,
                      RateControlMode: "CBR",
                      CodecProfile: "LC",
                      CodingMode: "CODING_MODE_2_0",
                      RawFormat: "NONE",
                      SampleRate: 48000,
                      Specification: "MPEG4",
                    },
                  },
                  LanguageCodeControl: "FOLLOW_INPUT",
                  AudioType: 0,
                },
              ],
              NameModifier: "_Ott_Hls_Ts_Avc_Aac_16x9_1920x1080p_8.5Mbps_qvbr",
            },
          ],
          OutputGroupSettings: {
            Type: "HLS_GROUP_SETTINGS",
            HlsGroupSettings: {
              ManifestDurationFormat: "INTEGER",
              SegmentLength: 3,
              TimedMetadataId3Period: 10,
              CaptionLanguageSetting: "OMIT",
              Destination: `${outputPath}/${"Apple HLS".replace(/\\s+/g, "")}${1}/`,
              TimedMetadataId3Frame: "PRIV",
              CodecSpecification: "RFC_4281",
              OutputSelection: "MANIFESTS_AND_SEGMENTS",
              ProgramDateTimePeriod: 600,
              MinSegmentLength: 0,
              DirectoryStructure: "SINGLE_DIRECTORY",
              ProgramDateTime: "EXCLUDE",
              SegmentControl: "SEGMENTED_FILES",
              ManifestCompression: "NONE",
              ClientCache: "ENABLED",
              StreamInfResolution: "INCLUDE",
            },
          },
        },
      ],
      AdAvailOffset: 0,
      Inputs: [
        {
          AudioSelectors: {
            "Audio Selector 1": {
              Offset: 0,
              DefaultSelection: "DEFAULT",
              ProgramSelection: 1,
            },
          },
          VideoSelector: {
            ColorSpace: "FOLLOW",
            Rotate: "DEGREE_0",
            AlphaBehavior: "DISCARD",
          },
          FilterEnable: "AUTO",
          PsiControl: "USE_PSI",
          FilterStrength: 0,
          DeblockFilter: "DISABLED",
          DenoiseFilter: "DISABLED",
          TimecodeSource: "ZEROBASED",
          FileInput: inputPath,
        },
      ],
    },
    AccelerationSettings: {
      Mode: "PREFERRED",
    },
    StatusUpdateInterval: "SECONDS_60",
    UserMetadata: metaData,
  }
}

const sendError = async (topic: string, stackName: string, logGroupName: string, err: Error) => {
  console.log(`Sending SNS error notification: ${err}`)
  const sns = new AWS.SNS({
    region: process.env.REGION,
  })
  try {
    const msg = {
      Details: `https://console.aws.amazon.com/cloudwatch/home?region=${process.env.AWS_REGION}#logStream:group=${logGroupName}`,
      Error: err,
    }
    await sns
      .publish({
        TargetArn: topic,
        Message: JSON.stringify(msg, null, 2),
        Subject: `${stackName}: Encoding Job Submit Failed`,
      })
      .promise()
  } catch (err) {
    console.error(err)
    throw err
  }
}
