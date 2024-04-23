import os, pyodbc
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
import io
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd

load_dotenv()


# Azure SQL Database connection string
server = os.environ.get('unlibrary1.database.windows.net')
database = os.environ.get('Unlibrary1')
username = os.environ.get('Unlibrary1')
password = os.environ.get('ladygagathZ7377@')
account_storage = os.environ.get('unlibrary')

# Using pyodbc
engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+18+for+SQL+Server')


class AzureDB():
    def __init__(self, local_path="./data", account_storage=account_storage):
        self.local_path = local_path
        self.account_url = f"https://{account_storage}.blob.core.windows.net"
        self.default_credential = DefaultAzureCredential()
        self.blob_service_client = BlobServiceClient(self.account_url, credential=self.default_credential)

    def access_container(self, container_name):
        # Use this function to create/access a new container
        try:
            # Creating container if not exist
            self.container_client = self.blob_service_client.create_container(container_name)
            print(f"Creating container {container_name} since not exist in database")
            self.container_name = container_name

        except Exception as ex:
            print(f"Acessing container {container_name}")
            # Access the container
            self.container_client = self.blob_service_client.get_container_client(container=container_name)
            self.container_name = container_name

    def delete_container(self):
        # Delete a container
        print("Deleting blob container...")
        self.container_client.delete_container()
        print("Done")

    def upload_blob(self, blob_name, blob_data=None):
        # Create a file in the local data directory to upload as blob to Azure
        local_file_name = blob_name
        upload_file_path = os.path.join(self.local_path, local_file_name)
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=local_file_name)
        print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)

        if blob_data is not None:
            blob_client.create_blob_from_text(container_name=self.container_name, blob_name=blob_name, text=blob_data)
        else:
            # Upload the created file
            with open(file=upload_file_path, mode="rb") as data:
                blob_client.upload_blob(data)

    def list_blobs(self):
        print("\nListing blobs...")
        # List the blobs in the container
        blob_list = self.container_client.list_blobs()
        for blob in blob_list:
            print("\t" + blob.name)

    def download_blob(self, blob_name):
        # Download the blob to local storage
        download_file_path = os.path.join(self.local_path, blob_name)
        print("\nDownloading blob to \n\t" + download_file_path)
        with open(file=download_file_path, mode="wb") as download_file:
            download_file.write(self.container_client.download_blob(blob_name).readall())

    def delete_blob(self, container_name: str, blob_name: str):
        # Deleting a blob
        print("\nDeleting blob " + blob_name)
        blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        blob_client.delete_blob()

    def access_blob_csv(self, blob_name):
        # Read the csv blob from Azure
        try:
            print(f"Acessing blob {blob_name}")

            df = pd.read_csv(io.StringIO(self.container_client.download_blob(blob_name).readall().decode('utf-8')))
            return df
        except Exception as ex:
            print('Exception:')
            print(ex)

    def upload_dataframe_sqldatabase(self, blob_name, blob_data):
        print("\nUploading to Azure SQL server as table:\n\t" + blob_name)
        blob_data.to_sql(blob_name, engine, if_exists='replace', index=False)
        primary = blob_name.replace('dim', 'id')
        if 'fact' in blob_name.lower():
            with engine.connect() as con:
                trans = con.begin()
                con.execute(text(f'ALTER TABLE [dbo].[{blob_name}] alter column {blob_name}_id bigint NOT NULL'))
                con.execute(text(
                    f'ALTER TABLE [dbo].[{blob_name}] ADD CONSTRAINT [PK_{blob_name}] PRIMARY KEY CLUSTERED ([{blob_name}_id] ASC);'))
                trans.commit()
        else:
            with engine.connect() as con:
                trans = con.begin()
                con.execute(text(f'ALTER TABLE [dbo].[{blob_name}] alter column {primary} bigint NOT NULL'))
                con.execute(text(
                    f'ALTER TABLE [dbo].[{blob_name}] ADD CONSTRAINT [PK_{blob_name}] PRIMARY KEY CLUSTERED ([{primary}] ASC);'))
                trans.commit()

    def append_dataframe_sqldatabase(self, blob_name, blob_data):
        print("\nAppending to table:\n\t" + blob_name)
        blob_data.to_sql(blob_name, engine, if_exists='append', index=False)

    def delete_sqldatabase(self, table_name):
        with engine.connect() as con:
            trans = con.begin()
            con.execute(text(f"DROP TABLE [dbo].[{table_name}]"))
            trans.commit()