import pandas as pd
import libsql_client
import asyncio
import re

"""
S3 to Turso Data Pipeline

This module provides functions to set up a data pipeline for transferring data from AWS S3 to a SQL database.
It includes functions to read Parquet files from an S3 bucket, create database tables based on DataFrame schemas,
and implement change data capture (CDC) by tracking insert, update, and delete operations in a historic table.

Functions:
    - read_s3(aws_access_key_id:str, aws_secret_access_key:str) -> pd.DataFrame:
        Reads Parquet files from an S3 path into a Pandas DataFrame.
    
    - obtain_schema(df:pd.DataFrame) -> str:
        Generates the schema based on the DataFrame columns and their data types.

    - stage(url:str, auth_token:str, table:str, df:pd.DataFrame, schema:str):
        Sets up the database environment by creating main and historic tables, along with triggers for CDC operations.

    - example_usage(url, auth_token, table, df):
        Demonstrates the usage of the setup by executing example SQL queries such as insert, update, and delete operations.

    - main(aws_access_key_id, aws_secret_access_key, url, auth_token):
        Orchestrates the entire data pipeline process by calling the above functions in sequence.


"""

def read_s3(aws_access_key_id:str, aws_secret_access_key:str) -> pd.DataFrame:
    # Read Parquet files from S3 path into a Pandas DataFrame
    df = pd.read_parquet(
        f"s3://AWS_S3_BUCKET/FOLDER/",
        engine='pyarrow',
        storage_options={
            'key': aws_access_key_id,
            'secret': aws_secret_access_key
        }
    )
    
    return df

def obtain_schema(df:pd.DataFrame) -> str:

    # Define a function to clean up dtype strings
    def clean_dtype(dtype_str):
        return re.sub(r'\[\w+\]|\d+', '', dtype_str)

    # Modify type_mapping to use cleaned up dtype strings
    type_mapping = {
        'int': 'INTEGER',
        'float': 'REAL',
        'object': 'TEXT',
        'string' : 'TEXT',
        'category': 'TEXT',
        'datetime': 'TIMESTAMP'
    }

    # Convert Pandas dtypes to SQLite types
    schema = ', '.join([f"{column} {type_mapping[clean_dtype(str(dtype))]}" for column, dtype in zip(df.columns, df.dtypes)])

    return schema


async def stage(url:str, auth_token:str, table:str, df:pd.DataFrame, schema:str):
    # Connect to the database using libsql_client asynchronously
    async with libsql_client.create_client(url=url, auth_token=auth_token) as client:

        columns =  ', '.join(df.columns)

        new_columns = f"(NEW.{', NEW.'.join(df.columns)}"
        old_columns = f"(OLD.{', OLD.'.join(df.columns)}"
        historic_table = f'{table}_HTB'
        
        await client.batch(
            [
        # Create main Table
        f"""
        CREATE TABLE IF NOT EXISTS {table}_TB (
            {schema}
        )
        """,
        # Create historic table.
        f"""
        CREATE TABLE IF NOT EXISTS {historic_table} (
            {schema}
            ,timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ,operation TEXT DEFAULT 'insert'
            ,validity INT
        )
        """,
        # Create trigger to check for uniqueness of ids.
        f"""
        CREATE TRIGGER IF NOT EXISTS {table}_ERRORS_TRIGGER BEFORE INSERT ON {table}_TB

        BEGIN
            SELECT
            CASE WHEN EXISTS (SELECT 1 FROM {historic_table} WHERE id = NEW.id) THEN
                RAISE (ABORT, 'ID already exists.')
            END;
        END;
        """, 
        # Create trigger for inserts to be tracked in historical table.
        f"""
        CREATE TRIGGER IF NOT EXISTS {table}_INSERT_TRIGGER AFTER INSERT ON {table}_TB

        FOR EACH ROW
        BEGIN
            INSERT INTO {historic_table} ({columns}, timestamp, operation)
            VALUES {new_columns}, CURRENT_TIMESTAMP, 'INSERT');
        END
        """, 
        # Create trigger for updates to be tracked in historical table.
        f"""
        CREATE TRIGGER IF NOT EXISTS {table}_UPDATE_TRIGGER AFTER UPDATE ON {table}_TB

        FOR EACH ROW
        BEGIN
            INSERT INTO {historic_table} ({columns}, timestamp, operation)
            VALUES {new_columns}, CURRENT_TIMESTAMP, 'UPDATE');
        END
        """,
        # Create trigger for deletes to be tracked in historical table.
         f"""
        CREATE TRIGGER IF NOT EXISTS {table}_DELETE_TRIGGER BEFORE DELETE ON {table}_TB

        FOR EACH ROW
        BEGIN
            INSERT INTO {historic_table} ({columns}, timestamp, operation)
            VALUES {old_columns}, CURRENT_TIMESTAMP, 'DELETE');
        END
        """,
        # Create trigger to validate data after each operation (insert/update/delete)
        f"""
        CREATE TRIGGER IF NOT EXISTS {table}_VALIDATE_TRIGGER AFTER INSERT ON {historic_table}
        FOR EACH ROW
        BEGIN
            UPDATE {historic_table} 
            SET validity = (CASE 
                        WHEN timestamp = (SELECT MAX(timestamp) FROM {table}_HTB WHERE id = NEW.id group by id ) THEN 1 
                        ELSE 0 
                    END)
        WHERE id = NEW.id;
        END
        """
            ]
        )


async def example_usage(url, auth_token, table, df):
    # Connect to the database using libsql_client asynchronously
    async with libsql_client.create_client(url=url, auth_token=auth_token) as client:

        # Convert DataFrame values to a bytes-like object
        values = df.to_records(index=False)
        values = ', '.join(str(x) for x in values)
        values = values.replace('None', "Null")
        values = values.replace("'NaT'", "Null")
        
        columns =  ', '.join(df.columns)
        
        # Define the batch queries with added delay between each query
        batch_queries = [
            f"""
            INSERT INTO {table}_TB ({columns})
            VALUES {values}
            """
            ,f"""
            INSERT INTO {table}_TB (id, numeric_col) values ('id4', 10)
            """
            ,f"""
            UPDATE {table}_TB SET numeric_col=5000000 where id='id4'
            """
            ,f"""
            DELETE FROM {table}_TB where id='id4'
            """
        ]
        
        # Iterate through the batch queries and add a delay between each query
        for query in batch_queries:
            await client.execute(query)
            await asyncio.sleep(1)  # Add 1 second delay between each query




async def main(aws_access_key_id, aws_secret_access_key, url, auth_token):

    # Get dataframe data.
    df = read_s3(aws_access_key_id, aws_secret_access_key)
    
    #  To create the table structure, obtain the schema from the df.
    schema = obtain_schema(df)

    # Create triggers for tracking cdc oeprations.
    await stage(url, auth_token, 'TABLE_NAME', df, schema)

    # Execute insert, update, delete to showcase usage.
    await example_usage(url, auth_token, 'TABLE_NAME', df)

loop = asyncio.get_event_loop()
loop.run_until_complete(
    main(
        aws_access_key_id = "AWS_ACCESS_KEY_ID", 
        aws_secret_access_key = "AWS_SECRET_ACCESS_KEY",
        url  = "TURSO_DB_URL", 
        auth_token = "TURSO_AUTH_TOKEN", 
    )
)
