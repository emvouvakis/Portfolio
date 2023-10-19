import datetime
import time
import pandas as pd
import boto3

"""

The goal is to find errors in Cloudwatch Logs for multiple groups in the last n_days.
If found, save the results in S3 and send custom email with SNS.

"""


# Create session with credentials
session = boto3.Session(
    aws_access_key_id='AWS_ACCESS_KEY_ID',
    aws_secret_access_key='AWS_SECRET_ACCESS_KEY',
    region_name='REGION'
)

# Create a Boto3 client for AWS CloudWatch Logs
cloudwatch_logs = session.client('logs')

# Initiate df
df = pd.DataFrame(columns=['logGroupName','timestamp', 'message','LogStream'])

# Calculate the date for the previous n_days to see results of both groups
n_days = 10
previous_day = datetime.date.today() - datetime.timedelta(days=n_days)
today = datetime.date.today()

# Calculate the start and end timestamps
start_time = datetime.datetime(previous_day.year, previous_day.month, previous_day.day, 0, 0, 0)
end_time = datetime.datetime(today.year, today.month, today.day, 23, 59, 59)
start_timestamp = int(time.mktime(start_time.timetuple()) * 1000)
end_timestamp = int(time.mktime(end_time.timetuple()) * 1000)

# Search in multiple log groups
for log_group_name in ['/aws/group1','/aws/group2']: # Insert your selected groups

    response = cloudwatch_logs.describe_log_streams(logGroupName=log_group_name, orderBy='LastEventTime', descending=True)

    if 'logStreams' in response:
        for log_stream in response['logStreams']:
            log_stream_name = log_stream['logStreamName']

            # Retrieve the logs for the specified time range
            logs = cloudwatch_logs.get_log_events(
                logGroupName=log_group_name,
                logStreamName=log_stream_name,
                startTime=start_timestamp,
                endTime=end_timestamp,
                startFromHead=True
            )

            for event in logs['events']:
                # Append to the DataFrame
                df = pd.concat([df, pd.DataFrame({'logGroupName': [log_group_name],'timestamp': [event['timestamp']], 'message': [event['message']], 'LogStream': [log_stream_name]})])

df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
df = df.sort_values('timestamp',ascending=False)

# Keep only the events with the Erros
df = df[df.apply(lambda row: 'ERROR' in row['message'], axis=1)]
df.reset_index(drop=True, inplace=True)

# If errors found then sent custom email an upload them in S3
if len(df)>1:
    # Get distinct values in the 'logGroupName'
    result = ', '.join(df['logGroupName'].unique())

    # Sent email with the failed groups
    sns = session.client('sns')

    sns.publish(
        TopicArn='TOPIC_ARN',
        Message=f'Errors Found in {result}',
        Subject='Status'
    )

    # Upload errors in S3
    s3 = session.client('s3') 
    transformed_data_csv = df.to_csv(index=False)

    s3.put_object(Bucket='BUCKET', Key='Errors.csv', Body=transformed_data_csv)
