import pandas as pd
import boto3
import io
from datetime import date

"""

This lambda function is triggered by an upload event in S3.
Reads the df and then apply ETL to save the transformed data in S3.
Send custom email with SNS.

"""



def lambda_handler(event, context):
    # Extract the bucket name and file key from the S3 event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']

    s3 = boto3.client('s3')
    # Read the CSV file from S3 into a Pandas DataFrame
    s3_object = s3.get_object(Bucket=bucket_name, Key=file_key)
    
    body = s3_object['Body'].read().decode('utf-8')  # Decode the bytes-like object to a string
    df = pd.read_csv(io.StringIO(body), names=['timestamp', 'character'], header=None, engine='python')
    
    # Create an empty dictionary to store status messages.
    msg = {}

    # Try to perform ETL on the DataFrame 'df'.
    try:
        transformed_data_csv = etl(df)
        msg['ETL'] = 'ETL successful.'
        
    except:
        # If an exception occurs during ETL, record an error message.
        msg['ETL'] = 'ETL failed.'

    # Get the current date for file naming.
    current_datetime = date.today()
    year = current_datetime.year
    month = current_datetime.month
    day = current_datetime.day
    
    # Specify the target S3 bucket and file path.
    target_bucket = 'BUCKET'  # Replace 'BUCKET' with the actual S3 bucket name.
    target_file_path = f'{year}_{month}_{day}.csv' # Define the target file path.
    
    # Try to save the transformed data as an object in an S3 bucket.
    try:
        s3.put_object(Bucket=target_bucket, Key=target_file_path, Body=transformed_data_csv)
        msg['S3'] = 'Transformed data saved in S3.'
    except:
        # If an exception occurs during S3 upload, record an error message.
        msg['S3'] = 'Transformed data failed to be saved in S3.'
        
    sender('ETL status: '+ msg['ETL'] + '\n' + 'S3 status: ' + msg['S3'] )


def sender(msg):
    # Create an SNS (Simple Notification Service) client using boto3.
    sns = boto3.client('sns')

    # Publish a message to a specified SNS topic.
    sns.publish(
        TopicArn='TOPIC_ARN',  # Replace 'TOPIC_ARN' with the actual ARN of the SNS topic.
        Message=msg,           # The message you want to send.
        Subject='TOPIC'        # The subject or topic of the message.
    )
    
    
def etl(df):
    # Insert your custom ETL 
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M:%S,%f') 
    
    return df.to_csv()