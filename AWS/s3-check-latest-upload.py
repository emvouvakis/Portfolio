import boto3
from datetime import date

"""

The goal is to check how many days have passed since the last upload to an S3 bucket.
If more than n_days have passed, upload file to selected bucket.

"""

try:
    # Create session with credentials
    session = boto3.Session(
        aws_access_key_id='AWS_ACCESS_KEY_ID',
        aws_secret_access_key='AWS_SECRET_ACCESS_KEY',
        region_name='REGION'
    )

    # Create an S3 client using the session
    s3_client = session.client('s3')

    # List objects in the bucket
    objects = s3_client.list_objects_v2(Bucket='BUCKET_NAME')

    # Sort the objects by modification date in descending order
    sorted_objects = sorted(objects.get('Contents', []), key=lambda x: x['LastModified'], reverse=True)

    # Specify number of days
    n_days = 30
    if abs(date.today() - sorted_objects[0]['LastModified'].date() ).days >= n_days :

        bucket = session.resource('s3').Bucket('BUCKET_NAME')

        current_datetime = date.today()
        year = current_datetime.year
        month = current_datetime.month
        day = current_datetime.day

        filename= f'{year}_{month}_{day}.csv'

        bucket.upload_file(Filename='logs.csv', Key=f'folder/{filename}')
except:
    pass






