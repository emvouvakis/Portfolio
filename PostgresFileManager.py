import os
import psycopg2.pool

class PostgresFileManager:
    """Handles file upload and download operations to a PostgreSQL database."""

    def __init__(self, host, dbname, user, password, table=None):
        """
        Initializes the PostgresFileManager class with database connection details.

        Args:
        - host: Hostname for the database.
        - dbname: Name of the database.
        - user: Username for database access.
        - password: Password for the specified user.
        - table: Name of the table to be used for file storage. Default is 'file_table'.
        """
        self.table = table or "file_table"

        # Establishes a connection pool for database connections
        self.connection_pool = psycopg2.pool.SimpleConnectionPool(
            minconn=1,
            maxconn=5,
            host=host,
            dbname=dbname,
            user=user,
            password=password
        )

        # Creates the table for file storage if it doesn't exist
        self.create_table_if_not_exists()

    def get_connection(self):
        """Gets a connection from the connection pool."""
        return self.connection_pool.getconn()

    def release_connection(self, conn):
        """Releases a connection back to the connection pool."""
        self.connection_pool.putconn(conn)

    def create_table_if_not_exists(self):
        """Creates the file storage table if it doesn't exist."""
        try:
            conn = self.get_connection()
            create_table_query = f'''
            CREATE TABLE IF NOT EXISTS {self.table} (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                file BYTEA,
                file_name VARCHAR,
                file_extension VARCHAR
            )
            '''
            with conn.cursor() as cursor:
                cursor.execute(create_table_query)
                conn.commit()
        except psycopg2.Error as e:
            # Handles any database errors
            raise e
        finally:
            self.release_connection(conn)

    def upload_file(self, dir_path=None, file=None):
        """
        Uploads a file to the database.

        Args:
        - dir_path: Directory path where the file is located. Default is the current working directory.
        - file: Name of the file to be uploaded.

        Raises:
        - ValueError: If the file parameter is not provided or if the file size exceeds 1GB limit.
        """
        if not file:
            raise ValueError("File parameter is required.")
        dir_path = dir_path or os.getcwd()

        file_path = os.path.join(dir_path, file)
        file_name = str(os.path.splitext(file)[0])
        file_extension = str(os.path.splitext(file)[1])

        # Check file size
        file_size = os.path.getsize(file_path)
        max_size = 1000 * 1024 * 1024  # 1GB in bytes
        if file_size > max_size:
            raise ValueError("File size exceeds 1GB limit.")

        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                with open(file_path, 'rb') as file_obj:
                    file_data = file_obj.read()

                insert_query = f"INSERT INTO {self.table} (file, file_name, file_extension) VALUES (%s, %s, %s)"
                cursor.execute(insert_query, (file_data, file_name, file_extension))
            conn.commit()
            self.release_connection(conn)
            print("File uploaded successfully.")
        except (psycopg2.Error, IOError) as e:
            # Handles database or IO errors
            raise e

    def download_file(self, file_id=None, download_path=None):
        """
        Downloads a file from the database.

        Args:
        - file_id: ID of the file to be downloaded.
        - download_path: Directory path where the file will be downloaded. Default is the current working directory.

        Raises:
        - ValueError: If the file_id parameter is not provided.
        """
        if not file_id:
            raise ValueError("Required 'file_id'.")
        download_path = download_path or os.getcwd()

        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                select_query = f"SELECT file, file_name, file_extension FROM {self.table} WHERE id = %s"
                cursor.execute(select_query, (file_id,))
                file_data = cursor.fetchone()

                if file_data:
                    file_content, file_name, file_extension = file_data
                    full_file_name = f"{file_name}{file_extension}"

                    download_file_path = os.path.join(download_path, full_file_name)

                    with open(download_file_path, 'wb') as file:
                        file.write(file_content)
                    print(f"File '{full_file_name}' with ID {file_id} downloaded successfully.")
                else:
                    print(f"File with ID {file_id} not found.")
            self.release_connection(conn)
        except (psycopg2.Error, IOError) as e:
            # Handles database or IO errors
            raise e

# Example usage
postgres = PostgresFileManager(
                host='your_postgres_host',
                dbname='your_postgres_name',
                user='your_postgres_user',
                password='your_postgres_password'
                )

postgres.upload_file(file = 'file.csv') 
postgres.download_file(1)
