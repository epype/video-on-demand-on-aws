/*********************************************************************************************************************
 *  Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                           *
 *                                                                                                                    *
 *  Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance    *
 *  with the License. A copy of the License is located at                                                             *
 *                                                                                                                    *
 *      http://www.apache.org/licenses/LICENSE-2.0                                                                    *
 *                                                                                                                    *
 *  or in the 'license' file accompanying this file. This file is distributed on an 'AS IS' BASIS, WITHOUT WARRANTIES *
 *  OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions    *
 *  and limitations under the License.                                                                                *
 *********************************************************************************************************************/

const expect = require('chai').expect;
const path = require('path');
const { mockClient } = require('aws-sdk-client-mock');
const { SQSClient, DeleteMessageBatchCommand } = require('@aws-sdk/client-sqs');
const { LambdaClient, InvokeCommand } = require('@aws-sdk/client-lambda');

const lambda = require('../index.js');

describe('#API::', () => {
    const _publishEvent = {
        Records: [
            {
                //messageId: '03538a05-2866-4fd5-aa3f-83c6db30dade',
                messageId: '12345',
                receiptHandle: 'AQEB5+CQNlfA2LTbo/Yp/eFLMn6ueLjjDd5KdxHgj0ZW4CimJ1bxpxhiibHwDZEsUhHuoO+g8i4gGZ7bmGgZkBaxnsNVJxwdNDBSuqTERcqwB22Uk9hOWlv1hi8cUg3d7D02kZb5cRgBmb3pyjKIUTooG3M6zWTqeIh0ECwYPy1HhITc7nkRnK0OJUS68Ca5aLxF2jEPmyzaEL89An0z/fVXzydmRYgxQoaxz48hZjjewjoT0m/sqiq7Em3MCIm4Wn/+ZB5buEXlWABJaaXPTgujYG4PuW44Xz14EprTSWnL44+BPMvl0H4IQXoaBawlZO6CvHoLs1i9x5C6iJcDoKjVEan5VlYvEwN+N18cV4uu7Y1IqFA7OXuZNZyFZ2Eue/bt',
                md5OfBody: '7f6ab085437ec9c741dd96152b5177b3',
                attributes: {},
                body: `{
                    "encodeJobId": "1589389574836-8xotiq",
                    "frameCapture": true,
                    "jobTemplate_2160p": "Epype-Video_Ott_2160p_Avc_Aac_16x9_qvbr",
                    "acceleratedTranscoding": "DISABLED",
                    "workflowTrigger": "Video",
                    "frameCaptureWidth": 1280,
                    "jobTemplate_1080p": "Epype-Video_Ott_1080p_Avc_Aac_16x9_qvbr",
                    "cloudFront": "dc1wxrnq9v1xb.cloudfront.net",
                    "archiveSource": "DISABLED",
                    "enableSqs": true,
                    "srcBucket": "source-bucket",
                    "inputRotate": "DEGREE_0",
                    "srcWidth": 640,
                    "destBucket": "destination-bucket",
                    "workflowStatus": "Complete",
                    "jobTemplate_720p": "Epype-Video_Ott_720p_Avc_Aac_16x9_qvbr",
                    "encodingJob": {
                        "Role": "arn:aws:iam::671657677816:role/Epype-Video-MediaConvertRole-CCS2T6NIOH54",
                        "UserMetadata": {
                            "workflow": "Epype-Video",
                            "guid": "12345678"
                        },
                        "JobTemplate": "Epype-Video_Ott_720p_Avc_Aac_16x9_qvbr",
                        "Settings": {
                            "Inputs": [
                                {
                                    "FilterStrength": 0,
                                    "DeblockFilter": "DISABLED",
                                    "TimecodeSource": "EMBEDDED",
                                    "VideoSelector": {
                                        "Rotate": "DEGREE_0",
                                        "ColorSpace": "FOLLOW"
                                    },
                                    "AudioSelectors": {
                                        "Audio Selector 1": {
                                            "DefaultSelection": "NOT_DEFAULT",
                                            "Tracks": [
                                                1
                                            ],
                                            "Offset": 0,
                                            "ProgramSelection": 1,
                                            "SelectorType": "TRACK"
                                        }
                                    },
                                    "FileInput": "s3://source-bucket/99992_1234_20200513002939.webm",
                                    "PsiControl": "USE_PSI",
                                    "DenoiseFilter": "DISABLED",
                                    "FilterEnable": "AUTO"
                                }
                            ],
                            "OutputGroups": [
                                {
                                    "Outputs": [],
                                    "OutputGroupSettings": {
                                        "FileGroupSettings": {
                                            "Destination": "s3://destination-bucket/12345678/mp4/"
                                        },
                                        "Type": "FILE_GROUP_SETTINGS"
                                    },
                                    "Name": "File Group"
                                },
                                {
                                    "Outputs": [],
                                    "OutputGroupSettings": {
                                        "HlsGroupSettings": {
                                            "SegmentLength": 5,
                                            "Destination": "s3://destination-bucket/12345678/hls/",
                                            "MinSegmentLength": 0
                                        },
                                        "Type": "HLS_GROUP_SETTINGS"
                                    },
                                    "Name": "HLS Group"
                                },
                                {
                                    "Outputs": [
                                        {
                                            "VideoDescription": {
                                                "TimecodeInsertion": "DISABLED",
                                                "DropFrameTimecode": "ENABLED",
                                                "ColorMetadata": "INSERT",
                                                "ScalingBehavior": "DEFAULT",
                                                "RespondToAfd": "NONE",
                                                "AntiAlias": "ENABLED",
                                                "AfdSignaling": "NONE",
                                                "Sharpness": 100,
                                                "CodecSettings": {
                                                    "FrameCaptureSettings": {
                                                        "FramerateNumerator": 1,
                                                        "MaxCaptures": 10000000,
                                                        "Quality": 80,
                                                        "FramerateDenominator": 5
                                                    },
                                                    "Codec": "FRAME_CAPTURE"
                                                }
                                            },
                                            "NameModifier": "_thumb",
                                                "ContainerSettings": {
                                                "Container": "RAW"
                                            }
                                        }
                                    ],
                                    "OutputGroupSettings": {
                                        "FileGroupSettings": {
                                            "Destination": "s3://destination-bucket/12345678/thumbnails/"
                                        },
                                        "Type": "FILE_GROUP_SETTINGS"
                                    },
                                    "CustomName": "Frame Capture",
                                    "Name": "File Group"
                                }
                            ]
                        }
                    },
                    "workflowName": "Epype-Video",
                    "encodingProfile": 720,
                    "isCustomTemplate": false,
                    "startTime": "2020-05-13T17:06:03.514Z",
                    "enableMediaPackage": false,
                    "frameCaptureHeight": 720,
                    "jobTemplate": "Epype-Video_Ott_720p_Avc_Aac_16x9_qvbr",
                    "enableSns": true,
                    "srcVideo": "99992_1234_20200513002939.webm",
                    "srcHeight": 480,
                    "encodingOutput": {
                        "version": "0",
                        "id": "bcae2cdd-c78c-28db-3636-ccaa6cf79dd3",
                        "detail-type": "MediaConvert Job State Change",
                        "source": "aws.mediaconvert",
                        "account": "671657677816",
                        "time": "2020-05-13T17:06:21Z",
                        "region": "us-east-1",
                        "resources": [
                            "arn:aws:mediaconvert:us-east-1:671657677816:jobs/1589389574836-8xotiq"
                        ],
                        "detail": {
                            "timestamp": 1589389581779,
                            "accountId": "671657677816",
                            "queue": "arn:aws:mediaconvert:us-east-1:671657677816:queues/Default",
                            "jobId": "1589389574836-8xotiq",
                            "status": "COMPLETE",
                            "userMetadata": {
                                "guid": "12345678",
                                "workflow": "Epype-Video"
                            },
                            "outputGroupDetails": [
                                {
                                    "outputDetails": [
                                        {
                                            "outputFilePaths": [
                                                "s3://destination-bucket/12345678/mp4/99992_1234_20200513002939_Mp4_Avc_Aac_16x9_1280x720p_24Hz_4.5Mbps_qvbr.mp4"
                                            ],
                                            "durationInMs": 1960,
                                            "videoDetails": {
                                                "widthInPx": 1280,
                                                "heightInPx": 720
                                            }
                                        }
                                    ],
                                    "type": "FILE_GROUP"
                                },
                                {
                                    "outputDetails": [
                                        {
                                            "outputFilePaths": [
                                                    "s3://destination-bucket/12345678/hls/99992_1234_20200513002939_Ott_Hls_Ts_Avc_Aac_16x9_480x270p_15Hz_0.4Mbps_qvbr.m3u8"
                                            ],
                                            "durationInMs": 2002,
                                            "videoDetails": {
                                                "widthInPx": 480,
                                                "heightInPx": 270
                                            }
                                        },
                                        {
                                            "outputFilePaths": [
                                                "s3://destination-bucket/12345678/hls/99992_1234_20200513002939_Ott_Hls_Ts_Avc_Aac_16x9_640x360p_30Hz_0.6Mbps_qvbr.m3u8"
                                            ],
                                            "durationInMs": 1968,
                                            "videoDetails": {
                                                "widthInPx": 640,
                                                "heightInPx": 360
                                            }
                                        },
                                        {
                                            "outputFilePaths": [
                                                "s3://destination-bucket/12345678/hls/99992_1234_20200513002939_Ott_Hls_Ts_Avc_Aac_16x9_640x360p_30Hz_1.2Mbps_qvbr.m3u8"
                                            ],
                                            "durationInMs": 1968,
                                            "videoDetails": {
                                                "widthInPx": 640,
                                                "heightInPx": 360
                                            }
                                        },
                                        {
                                            "outputFilePaths": [
                                                "s3://destination-bucket/12345678/hls/99992_1234_20200513002939_Ott_Hls_Ts_Avc_Aac_16x9_960x540p_30Hz_3.5Mbps_qvbr.m3u8"
                                            ],
                                            "durationInMs": 1968,
                                            "videoDetails": {
                                                "widthInPx": 960,
                                                "heightInPx": 540
                                            }
                                        },
                                        {
                                            "outputFilePaths": [
                                                "s3://destination-bucket/12345678/hls/99992_1234_20200513002939_Ott_Hls_Ts_Avc_Aac_16x9_1280x720p_30Hz_3.5Mbps_qvbr.m3u8"
                                            ],
                                            "durationInMs": 1968,
                                            "videoDetails": {
                                                "widthInPx": 1280,
                                                "heightInPx": 720
                                            }
                                        },
                                        {
                                            "outputFilePaths": [
                                                "s3://destination-bucket/12345678/hls/99992_1234_20200513002939_Ott_Hls_Ts_Avc_Aac_16x9_1280x720p_30Hz_5.0Mbps_qvbr.m3u8"
                                            ],
                                            "durationInMs": 1968,
                                            "videoDetails": {
                                                "widthInPx": 1280,
                                                "heightInPx": 720
                                            }
                                        },
                                        {
                                            "outputFilePaths": [
                                                "s3://destination-bucket/12345678/hls/99992_1234_20200513002939_Ott_Hls_Ts_Avc_Aac_16x9_1280x720p_30Hz_6.5Mbps_qvbr.m3u8"
                                            ],
                                            "durationInMs": 1968,
                                            "videoDetails": {
                                                "widthInPx": 1280,
                                                "heightInPx": 720
                                            }
                                        }
                                    ],
                                    "playlistFilePaths": [
                                        "s3://destination-bucket/12345678/hls/99992_1234_20200513002939.m3u8"
                                    ],
                                    "type": "HLS_GROUP"
                                },
                                {
                                    "outputDetails": [
                                        {
                                            "outputFilePaths": [
                                                "s3://destination-bucket/12345678/thumbnails/99992_1234_20200513002939_thumb.0000000.jpg"
                                            ],
                                            "durationInMs": 5000,
                                            "videoDetails": {
                                                "widthInPx": 640,
                                                "heightInPx": 480
                                            }
                                        }
                                    ],
                                    "type": "FILE_GROUP"
                                }
                            ]
                        }
                    },
                    "endTime": "2020-05-13T17:06:24.553Z",
                    "mp4Outputs": [
                        "s3://destination-bucket/12345678/mp4/99992_1234_20200513002939_Mp4_Avc_Aac_16x9_1280x720p_24Hz_4.5Mbps_qvbr.mp4"
                    ],
                    "mp4Urls": [
                        "https://dc1wxrnq9v1xb.cloudfront.net/12345678/mp4/99992_1234_20200513002939_Mp4_Avc_Aac_16x9_1280x720p_24Hz_4.5Mbps_qvbr.mp4"
                    ],
                    "hlsPlaylist": "s3://destination-bucket/12345678/hls/99992_1234_20200513002939.m3u8",
                    "hlsUrl": "https://dc1wxrnq9v1xb.cloudfront.net/12345678/hls/99992_1234_20200513002939.m3u8",
                    "thumbNails": [
                        "s3://destination-bucket/12345678/thumbnails/99992_1234_20200513002939_thumb.0000000.jpg"
                    ],
                    "thumbNailsUrls": [
                        "https://dc1wxrnq9v1xb.cloudfront.net/12345678/thumbnails/99992_1234_20200513002939_thumb.0000000.jpg"
                    ],
                    "guid": "12345678"
                }`
            }
        ]
    };

    const _errorEvent = {
        Records: [
            {
                messageId: '03538a05-2866-4fd5-aa3f-83c6db30dade',
                receiptHandle: 'AQEB5+CQNlfA2LTbo/Yp/eFLMn6ueLjjDd5KdxHgj0ZW4CimJ1bxpxhiibHwDZEsUhHuoO+g8i4gGZ7bmGgZkBaxnsNVJxwdNDBSuqTERcqwB22Uk9hOWlv1hi8cUg3d7D02kZb5cRgBmb3pyjKIUTooG3M6zWTqeIh0ECwYPy1HhITc7nkRnK0OJUS68Ca5aLxF2jEPmyzaEL89An0z/fVXzydmRYgxQoaxz48hZjjewjoT0m/sqiq7Em3MCIm4Wn/+ZB5buEXlWABJaaXPTgujYG4PuW44Xz14EprTSWnL44+BPMvl0H4IQXoaBawlZO6CvHoLs1i9x5C6iJcDoKjVEan5VlYvEwN+N18cV4uu7Y1IqFA7OXuZNZyFZ2Eue/bt',
                md5OfBody: '7f6ab085437ec9c741dd96152b5177b3',
                attributes: {},
                body: `{
                    "guid": "12345678",
                    "startTime": "now",
                    "srcVideo": "video_file.mp4"
                }`
            }
        ]
    };

    process.env.ErrorHandler = 'error_handler';
    process.env.SqsQueue = 'https://sqs.amazonaws.com/1234';
    process.env.EpypeApiUrl = 'https://api.epype.io';
    process.env.EpypeApiAuthorization = '1234';

    const lambdaClientMock = mockClient(LambdaClient);
    const sQSClientMock = mockClient(SQSClient);

    afterEach(() => sQSClientMock.reset());

    it('should remove properties when MediaPackage is enabled', async() => {
        sQSClientMock.on(DeleteMessageBatchCommand).resolves();
        lambdaClientMock.on(InvokeCommand).resolves();

        const _event = {
            Records: [
                {
                    messageId: '03538a05-2866-4fd5-aa3f-83c6db30dade',
                    receiptHandle: 'AQEB5+CQNlfA2LTbo/Yp/eFLMn6ueLjjDd5KdxHgj0ZW4CimJ1bxpxhiibHwDZEsUhHuoO+g8i4gGZ7bmGgZkBaxnsNVJxwdNDBSuqTERcqwB22Uk9hOWlv1hi8cUg3d7D02kZb5cRgBmb3pyjKIUTooG3M6zWTqeIh0ECwYPy1HhITc7nkRnK0OJUS68Ca5aLxF2jEPmyzaEL89An0z/fVXzydmRYgxQoaxz48hZjjewjoT0m/sqiq7Em3MCIm4Wn/+ZB5buEXlWABJaaXPTgujYG4PuW44Xz14EprTSWnL44+BPMvl0H4IQXoaBawlZO6CvHoLs1i9x5C6iJcDoKjVEan5VlYvEwN+N18cV4uu7Y1IqFA7OXuZNZyFZ2Eue/bt',
                    md5OfBody: '7f6ab085437ec9c741dd96152b5177b3',
                    attributes: {},
                    body: `{
                        "guid": "12345678",
                        "srcVideo": "video_file.mp4",
                        "workflowStatus": "Complete",
                        "enableMediaPackage": true,
                        "mp4Outputs": ["mp4-output-1"],
                        "mp4Urls": ["mp4-url-1"],
                        "hlsPlaylist": "hls-playlist",
                        "hlsUrl": "hls-url",
                        "dashPlaylist": "dash-playlist",
                        "dashUrl": "dash-url",
                        "mssPlaylist": "mss-playlist",
                        "mssUrl": "mss-url",
                        "cmafDashPlaylist": "cmaf-dash-playlist",
                        "cmafDashUrl": "cmaf-dash-url",
                        "cmafHlsPlaylist": "cmaf-hls-playlist",
                        "cmafHlsUrl": "cmaf-hls-url"
                    }`
                }
            ]
        };

        const response = await lambda.handler(_event);
        expect(response.Records[0].mp4Outputs).to.be.undefined;
        expect(response.Records[0].mp4Urls).to.be.undefined;
        expect(response.Records[0].hlsPlaylist).to.be.undefined;
        expect(response.Records[0].hlsUrl).to.be.undefined;
        expect(response.Records[0].dashPlaylist).to.be.undefined;
        expect(response.Records[0].dashUrl).to.be.undefined;
        expect(response.Records[0].mssPlaylist).to.be.undefined;
        expect(response.Records[0].mssUrl).to.be.undefined;
        expect(response.Records[0].cmafDashPlaylist).to.be.undefined;
        expect(response.Records[0].cmafDashUrl).to.be.undefined;
        expect(response.Records[0].cmafHlsPlaylist).to.be.undefined;
        expect(response.Records[0].cmafHlsUrl).to.be.undefined;
    });

    it('should return "ERROR" when workflowStatus is not completed', async () => {
        sQSClientMock.on(DeleteMessageBatchCommand).resolves();
        lambdaClientMock.on(InvokeCommand).resolves();

        await lambda.handler(_errorEvent).catch(err => {
            expect(err.toString()).to.equal('Error: Workflow Status not completed.');
        });
    });

    it('should return "SQS ERROR" when sqs deleteMessageBatch fails', async () => {
        sQSClientMock.on(DeleteMessageBatchCommand).rejects('SQS Error');
        lambdaClientMock.on(InvokeCommand).resolves();

        await lambda.handler(_publishEvent).catch(err => {
            expect(err.toString()).to.equal('SQS ERROR');
        });
    });
});
