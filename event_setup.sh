#!/bin/sh

export AWS_PROFILE=${AWS_PROFILE:-"mbta-history"}

mode="dev"
fn="summarize"
fn_name="$AWS_PROFILE-$mode-$fn"
region="us-east-2"
lambda_arn=$(aws lambda list-functions --region $region | jq -r '.Functions | map(select(.FunctionName == "'$fn_name'").FunctionArn)[0]')
account_id=$(aws sts get-caller-identity | jq -r '.Account')

aws lambda add-permission \
    --function-name "$fn_name" \
    --region us-east-2 \
    --statement-id "run-summarize-on-upload" \
    --action "lambda:InvokeFunction" \
    --principal s3.amazonaws.com \
    --source-arn arn:aws:s3:::mbta-history.apptic.xyz \
    --source-account $account_id

aws s3api put-bucket-notification-configuration \
    --bucket "mbta-history.apptic.xyz" \
    --notification-configuration '
{
  "LambdaFunctionConfigurations":
  [
    {
      "Id": "summarize-on-upload",
      "LambdaFunctionArn": "'$lambda_arn'",
      "Events": ["s3:ObjectCreated:*"],
      "Filter": {
        "Key": {
        "FilterRules": [
          {
             "Name": "suffix",
             "Value": ".csv.gz"
          }
        ]
      }
    }
    }
  ]
}
'
