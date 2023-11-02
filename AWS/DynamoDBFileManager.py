import boto3
import os
import base64
import pandas as pd
import time
from botocore.exceptions import ClientError
import logging

class DynamoDBFileManager:

    """
    A class for uploading and downloading CSV files to/from Amazon DynamoDB.

    Args:
        aws_access_key_id (str): The AWS access key ID for authentication.
        aws_secret_access_key (str): The AWS secret access key for authentication.
        region_name (str): The AWS region where DynamoDB is located.
        table_name (str): The name of the selected DynamoDB table.
        selected_date (int): The selected date in YYYYMMDD format as an integer.

    Attributes:
        session (boto3.Session): The Boto3 session for AWS authentication and resource management.
        dynamodb (boto3.client): The DynamoDB client for database operations.

    Methods:
        - does_table_exist(): Checks if the specified DynamoDB table exists.
        - create_table(): Creates a DynamoDB table for data storage.
        - upload_file(file_path): Uploads a file to DynamoDB in Parquet format with ZIP compression.
        - download_file(output_dir): Downloads a Parquet file from DynamoDB and saves it to the output directory.     

    Data Transformation:
    - Data Conversion: When uploading a file, it reads data from a text/csv file, converts it to Parquet format, and applies ZIP compression to the data for storage efficiency.
    - Chunking: If the data size exceeds 400KB, it automatically splits the data into smaller chunks and uploads them individually.
    - Download and Transformation: When downloading a file, the class retrieves data from DynamoDB and reconstructs it into Parquet format.

    Example Usage:
    ```python
    file_manager = DynamoDBFileManager(aws_access_key_id, aws_secret_access_key, region_name, 'your_table_name', selected_date)
    file_manager.upload_file('input.txt')
    file_manager.download_file('output_directory')
    """


    def __init__(self, aws_access_key_id, aws_secret_access_key, region_name, table_name, selected_date):
        # Initialize the Boto3 session with the provided credentials and region
        self.session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )

        # Initialize the DynamoDB client using the session
        self.dynamodb = self.session.client('dynamodb')

        # Get the current date in YYYYMMDD format as a number
        self.selected_date = selected_date

        # Declare the target table
        self.table_name=table_name

        # Check if table exists
        if not self.does_table_exist():
            self.create_table()


    def does_table_exist(self):
        """
        Checks if a DynamoDB table exists.

        :return: True if the table exists, False otherwise.
        """

        try:
            response = self.dynamodb.describe_table(TableName=self.table_name)
            if response and 'Table' in response:
                return True
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                return False
            else:
                # Handle other exceptions if necessary
                raise

        return False


    def create_table(self):
        """
        Creates an Amazon DynamoDB table for storing data.

        The table uses 'DateID' as the partition key and 'ChunkID' as the sort key.

        :return: The newly created table name.
        """
        try:
            # Initialize the DynamoDB resource
            self.dynamodb_r = self.session.resource('dynamodb')

            # Create the DynamoDB table with specified attributes
            self.table = self.dynamodb_r.create_table(
                TableName=self.table_name,
                KeySchema=[
                    {"AttributeName": "DateID", "KeyType": "HASH"},  # Partition key
                    {"AttributeName": "ChunkID", "KeyType": "RANGE"},  # Sort key
                ],
                AttributeDefinitions=[
                    {"AttributeName": "DateID", "AttributeType": "N"},
                    {"AttributeName": "ChunkID", "AttributeType": "N"}
                ],
                ProvisionedThroughput={
                    "ReadCapacityUnits": 10,
                    "WriteCapacityUnits": 10,
                },
                BillingMode='PROVISIONED'
            )
            self.table.wait_until_exists()
        except ClientError as err:
            # Handle exceptions and log the error
            error_message = f"Couldn't create table {self.table_name}. Error: {err.response['Error']['Code']}: {err.response['Error']['Message']}"
            logging.error(error_message)
            raise
        else:
            print(f'Created table: {self.table_name}')


    def upload_file(self, file_path):

        """
        Uploads a file to DynamoDB.

        Args:
            file_path (str): The path to the text file you want to upload.

        """
        # Read data from the CSV file and convert it to Parquet format
        df = pd.read_csv(file_path, delimiter=': ',engine='python')  #Change to custom delimeter
        parquet_df = df.to_parquet(compression='gzip')

        # Encode the Parquet content as base64
        encoded_content = base64.b64encode(parquet_df).decode('utf-8')

        # Check if the file size exceeds 400KB
        if len(encoded_content) > 400 * 1000:
            is_chunked = True
            chunk_size = 400 * 1000
            chunks = [encoded_content[i:i+chunk_size] for i in range(0, len(encoded_content), chunk_size)]
        else:
            is_chunked = False
            chunks = [encoded_content]

        # Upload each chunk to DynamoDB with the updated ChunkID value
        for index, chunk in enumerate(chunks, start=1):
            # Create a dictionary containing the data to be uploaded to DynamoDB
            item = {
                'Binary': {'B': chunk},
                'ChunkID': {'N': str(index)} if is_chunked else {'N': str(0)},  # Set to None if not chunked
                'DateID': {'N': str(self.selected_date)}
            }

            # Put the item in the DynamoDB table
            self.dynamodb.put_item(TableName= self.table_name, Item=item, ConditionExpression="attribute_not_exists(DateID)")
            print(f'Uploaded DateID: {item["DateID"]["N"]} ChunkID: {item["ChunkID"]["N"]}')
             # Sleep for a short duration before the next check
            time.sleep(5)


    def download_file(self, output_dir):

        """
        Downloads a file from DynamoDB and saves it to the output directory.

        Args:
            output_dir (str): The directory where the downloaded file will be saved.

        """

         # Create a directory to store the downloaded files
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        content_dict = {}
        last_evaluated_key = {}

        while True:
            scan_params = {
                'TableName': self.table_name,
                'FilterExpression': 'DateID = :date',
                'ExpressionAttributeValues': {':date': {'N': str(self.selected_date)}}
            }

            if last_evaluated_key:
                scan_params['ExclusiveStartKey'] = last_evaluated_key

            response = self.dynamodb.scan(**scan_params)

            items = response.get('Items', [])

            for item in items:
                chunk_id = item['ChunkID']['N']
                content = base64.b64decode(item['Binary']['B'])
                content_dict[chunk_id] = content

            if 'LastEvaluatedKey' in response:
                last_evaluated_key = response['LastEvaluatedKey']
            else:
                break
        
        # Sort the content_dict by ChunkID
        sorted_content = {k: content_dict[k] for k in sorted(content_dict, key=lambda k: int(k))}
        # Join the bytes inside the content
        joined_content = b''.join(sorted_content.values())

        # Write the concatenated content to a file in the specified output directory
        file_path = os.path.join(output_dir, 'downloaded_file.parquet')
        with open(file_path, 'wb') as file:
            file.write(joined_content)

        print(f"Downloaded file to {file_path}")


manager = DynamoDBFileManager(aws_access_key_id='AWS_ACCESS_KEY_ID',
                            aws_secret_access_key='AWS_SECRET_ACCESS_KEY',
                            region_name='REGION',
                            table_name='SELECTED_TABLE',
                            selected_date=20231102 #YYYYMMDD
                            )   

manager.upload_file('data.csv')
manager.download_file('output_directory')
