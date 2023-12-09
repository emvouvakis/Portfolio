import libsql_client
import os

class TursoFileManager:
    def __init__(self, url, auth_token, table=None):
        """
        Initializes the TursoFileManager with the specified parameters.

        Args:
        - url (str): URL of the database.
        - auth_token (str): Authentication token for accessing the database.
        - table (str, optional): Name of the table for file storage. Defaults to 'file_table'.
        """

        self.url = url
        self.auth_token = auth_token

        self.table = table or "file_table"
        self.client_sync = None
        self.create_table_if_not_exists()

    def connect_to_database(self):
        """
        Connects to the database using the provided URL and authentication token.
        """

        self.client_sync = libsql_client.create_client_sync(
            url=self.url,
            auth_token=self.auth_token
        )
        
    def disconnect_from_database(self):
        """
        Disconnects from the database.
        """

        if self.client_sync:
            self.client_sync.close()

    def create_table_if_not_exists(self):
        """
        Creates the table for file storage if it doesn't exist.
        """

        self.connect_to_database()

        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.table} (
            id INTEGER PRIMARY KEY,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            file BLOB,
            file_name TEXT,
            file_extension TEXT
        )
        """
        self.client_sync.execute(create_table_query)

        self.disconnect_from_database()

    def upload_file(self, dir_path=None, file=None):
        """
        Uploads a file to the database.
        
        Args:
        - dir_path (str, optional): Directory path of the file. Defaults to current working directory.
        - file (str): Name of the file to upload.
        
        Raises:
        - ValueError: If the file parameter is not provided or if the file size exceeds 1GB limit.
        """

        if not file:
            raise ValueError("File parameter is required.")
        
        # Set default directory path if not provided
        dir_path = dir_path or os.getcwd()
        file_path = os.path.join(dir_path, file)
        
        # Check file size
        file_size = os.path.getsize(file_path)
        max_size = 1000 * 1024 * 1024  # 1GB in bytes

        if file_size > max_size:
            raise ValueError("File size exceeds 1GB limit.")      

        # Extract file name and extension
        file_name, file_extension = os.path.splitext(file)

        # Connect to the database
        self.connect_to_database()

        # Read file content
        with open(file_path, 'rb') as file_obj:
            file_data = file_obj.read()

        # Insert file details into the database
        insert_query = f"""
        INSERT INTO {self.table} (file, file_name, file_extension) 
        VALUES (?, ?, ?)
        """
        self.client_sync.execute(insert_query, (file_data, file_name, file_extension))
        
        # Disconnect from the database
        self.disconnect_from_database()

    def download_file(self, file_id=None, download_path=None):
        """
        Downloads a file from the database.
        
        Args:
        - file_id (int): ID of the file to download.
        - download_path (str, optional): Directory path to save the downloaded file. Defaults to current working directory.
        """

        if not file_id:
            raise ValueError("Required 'file_id'.")

        # Connect to the database
        self.connect_to_database()
        
        # Set default download directory if not provided
        download_path = download_path or os.getcwd()
        
        # Prepare SQL query to retrieve file details
        select_query = f"""
        SELECT file, file_name, file_extension 
        FROM {self.table} 
        WHERE id = ?
        """
        result = self.client_sync.execute(select_query, (file_id,))

        if result:
            file_content, file_name, file_extension = result[0]
            full_file_name = f"{file_name}{file_extension}"
            download_file_path = os.path.join(download_path, full_file_name)

            # Write file content to the specified download path
            with open(download_file_path, 'wb') as file:
                file.write(file_content)
            print(f"File '{full_file_name}' with ID {file_id} downloaded successfully.")
        else:
            print(f"File with ID {file_id} not found.")
        
        # Disconnect from the database
        self.disconnect_from_database()

    def drop_table(self, table_to_delete=None):
        """
        Drops the specified table from the database.
        """
        
        if not table_to_delete:
            raise ValueError("Required 'table_to_delete'.")

        self.connect_to_database()

        drop_table_query = f"DROP TABLE IF EXISTS {table_to_delete}"
        self.client_sync.execute(drop_table_query)

        self.disconnect_from_database()

# Example usage
turso = TursoFileManager(
    url="your_turso_url",
    auth_token="your_turso_auth_token"
    )

turso.upload_file(file='data.csv')
turso.download_file(file_id=1)
turso.drop_table('example_table')
