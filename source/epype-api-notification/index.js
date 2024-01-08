/*********************************************************************************************************************
 *  Copyright 2024 EPYPE LLC or its affiliates. All Rights Reserved.                                                  *
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

const { SQS } = require("@aws-sdk/client-sqs");
const error = require('./lib/error.js');
const axios = require('axios');
const { URL } = require('url');

const NOT_APPLICABLE_PROPERTIES = [
    'srcMediainfo',
    'jobTemplate_2160p',
    'jobTemplate_1080p',
    'jobTemplate_720p',
    'jobTemplate_2160p_portrait',
    'jobTemplate_1080p_portrait',
    'jobTemplate_720p_portrait',
    'encodingJob',
    'encodingOutput'
];

const NOT_APPLICABLE_PROPERTIES_MEDIA_PACKAGE = [
    'mp4Outputs',
    'mp4Urls',
    'hlsPlaylist',
    'hlsUrl',
    'dashPlaylist',
    'dashUrl',
    'mssPlaylist',
    'mssUrl',
    'cmafDashPlaylist',
    'cmafDashUrl',
    'cmafHlsPlaylist',
    'cmafHlsUrl'
];

exports.handler = async (event) => {
    console.log(`REQUEST:: ${JSON.stringify(event, null, 2)}`);

    const sqs = new SQS({
        region: process.env.AWS_REGION
    });

    const entries = [];

    try {
        for (const record of event.Records) {
            const data = JSON.parse(record.body);

            if (data.workflowStatus === 'Complete') {
                NOT_APPLICABLE_PROPERTIES.forEach((prop) => {
                    delete data[prop];
                });

                /*
                If MediaPackage VOD is enabled, some properties can be removed from the final output.
                They're still saved in DynamoDB, but are not included in the notification.
                */
                if (data.enableMediaPackage) {
                    NOT_APPLICABLE_PROPERTIES_MEDIA_PACKAGE.forEach((prop) => {
                        delete data[prop];
                    });
                }
            } else {
                throw new Error('Workflow Status not completed.');
            }

            try {
                /*
                Replace the subdomain of the API if applicable, e.g. from a dev environment.
                If a different API subdomain is to be used the video filename S3 key will be prefixed by
                `${subdomain}/`
                */
                let apiUrl = new URL(process.env.EpypeApiUrl),
                    apiHeaders = {
                        'Content-Type': 'application/json',
                        Authorization: process.env.EpypeApiAuthorization
                    },
                    srcVideo = data.srcVideo,
                    filenameSlashIndex = srcVideo.indexOf('/');
                if (filenameSlashIndex !== -1) {
                    const tmpSrcVideo = srcVideo.split('/');
                    apiUrl.hostname = apiUrl.hostname.replace(apiUrl.hostname.split('.')[0], tmpSrcVideo[0]);
                    srcVideo = tmpSrcVideo[1];
                }
                // the video file name is ${user_id}_${video_type}_${video_id}_${date}.${extension}
                let filename_split = srcVideo.split('.')[0].split('_');
                let userId = filename_split[0],
                    videoType = filename_split[1],
                    videoId = filename_split[2];

                data.videoType = videoType;

                // URL.href includes the trailing slash
                console.log(`CALL API:: ${apiUrl.href}aws/videos/${videoId}:: ${JSON.stringify(data, null, 2)}`);
                // videoType can be one of user-vlp, org-vlp, org-video
                switch (videoType.split('-')[0]) {
                    case 'user':
                        apiHeaders['X-epype-user-id'] = userId;
                        break;
                    case 'org':
                        apiHeaders['X-epype-organization-id'] = userId;
                        break;
                }
                let response = await axios.post(
                    `${apiUrl.href}aws/videos/${videoId}`,
                    data,
                    {
                        headers: apiHeaders
                    }
                );
                // console.log(response.data);
                if (response.data?.success !== 1) {
                    console.error('Could not fetch data from API');
                    continue;
                }
                entries.push({Id: record.messageId, ReceiptHandle: record.receiptHandle});
            } catch (error) {
                console.error('API error occurred: ' + error);
                console.log(error.response.data);
            }
        }

        if (entries.length > 0) {
            let params = {
                Entries: entries,
                QueueUrl: process.env.SqsQueue
            };

            await sqs.deleteMessageBatch(params).promise();
        }
    } catch (err) {
        await error.handler(event, err);
        throw err;
    }

    return event;
};
