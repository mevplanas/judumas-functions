# Importing libraries
from azure.data.tables import TableServiceClient, TableClient
from azure.storage.blob import BlobServiceClient
from azure.storage.queue import (
    QueueClient,
    BinaryBase64EncodePolicy,
    BinaryBase64DecodePolicy,
)

from pathlib import Path
import os
import yaml
import pandas as pd
import io

# Import custom utils
from src import utils


class OpTableHandler(object):
    """
    Initialazing  Azure Table Service client
    """

    def __init__(self):
        # Initializing current path
        current_path = Path().resolve()

        # Open confiuration file as a stream
        with open((os.path.join(current_path, "configuration.yml")), "r") as stream:
            config = yaml.safe_load(stream=stream)

        # Loading Azure Storage credentials
        conn_str = config["AZURE_STORAGE"]["conn_string"]
        # Connecting to azure table service client via connection string
        self.service_client = TableServiceClient.from_connection_string(
            conn_str=conn_str
        )

    def _table_client(self, table_name: str) -> TableClient:
        """
        Description
        --
        Connecting to azure table

        Parameters
        --
        :param table_name : Azure Storage table name

        Ouput
        --
        table_client : Azure Storage Table client

        """
        table_client = self.service_client.get_table_client(table_name=table_name)

        return table_client

    def query_entities(self, table_name: str, query: str) -> list:
        """
        Description
        --
        The function query data from Azure Storage table and
        retrieves OSM street network

        Parameters
        --
        :param table_name : Azure Storage Table name
        :param query : Azure Table Storage queries
        https://learn.microsoft.com/en-us/rest/api/storageservices/Query-Operators-Supported-for-the-Table-Service?redirectedfrom=MSDN

        Output
        --
        entities_list : data queried from Table Storage
        """

        table_client = self._table_client(table_name)
        entities = table_client.query_entities(query)

        entities_list = []
        for entity in entities:
            entities_list.append(entity)

        return entities_list

    def inset_into_table(self, table_name: str, entities: list) -> None:
        """
        Description
        --
        The method insert data into Azure Storage Table

        Parameters
        --
        :param table_name : Azure Table Storage table name
        :param entities : list of entities that will be inserted
        """

        # Connect to Azure Storage Table
        table_client = self._table_client(table_name)
        # Create empty list for storing insert operations
        operations = []
        # Iterate to each entity
        for entity in entities:
            # Create new dictionary for data insertion
            coverted_entity = {}
            for key, value in entity.items():
                coverted_entity[key] = str(value)
            # Create operation dictionarty for data insertion
            operations.append(("upsert", coverted_entity))

        # Insert data into Azure Table Storage
        table_client.submit_transaction(operations)

    def create_entity(self, my_entity: dict) -> dict:
        """
        Description
        --
        Create single entity for upsert operation.

        Parameters
        --
        :param my_entity : dictionary item for data insertion

        Output
        --
        converted_entity : entity that will be used in upsert operation
        """

        date_query = utils.get_time()[0]
        my_entity.update({"PartitionKey": date_query})
        my_entity.update({"RowKey": utils.create_random(len_str=8)})

        coverted_entity = {}
        for key, value in my_entity.items():
            coverted_entity[key] = str(value)

        return coverted_entity


class OpBlobHandler(object):
    """
    Initialazing  Azure Blob Service client
    """

    def __init__(self):
        current_path = Path().resolve()

        with open((os.path.join(current_path, "configuration.yml")), "r") as stream:
            config = yaml.safe_load(stream=stream)

        conn_str = config["AZURE_STORAGE"]["conn_string"]
        self.service_client = BlobServiceClient.from_connection_string(
            conn_str=conn_str
        )

    def upload_blob_csv(self, container: str, name: str, data: list) -> None:
        """
        Description
        --
        The method upload csv file into Azure Blob Storage

        Parameters
        --
        :param container : Azure Blob Storage container
        :param name : filename that will be stored in container
        :param data : data that will be stored in csv file.
        """

        # Create dataframe
        df = pd.DataFrame.from_dict(data)

        # Creating StringIO class for storing data as bytes
        output = io.StringIO()
        # Convert dataframe into csv file and store it in memory
        output = df.to_csv()  #  test it without this

        # Connecting to Azure Blob Service client
        # and define csv file name for storing
        blob_client = self.service_client.get_blob_client(
            container=container, blob=f"{name}.csv"
        )

        # Append data to output data
        output = df.to_csv(index=False, encoding="utf-8")
        # Upload csv files to Azure Blob Storage
        blob_client.upload_blob(output, blob_type="BlockBlob")

    def read_csv(self, container: str, blob_name: str) -> pd.DataFrame:
        """
        Description
        --
        The method read csv file and returns pandas DataFrame

        Parameters
        --
        :param container : Azure Blob Storage container name
        :param blob_name : csv blob file name in blob storage

        Output
        --
        df : data converted into pandas dataframe
        """

        # Connect to Azure Blob Service client
        blob_client = self.service_client.get_blob_client(
            container=container, blob=blob_name
        )

        # Reading csv file from blob bytes into dataframe
        with io.BytesIO() as input_blob:
            blob_client.download_blob().download_to_stream(input_blob)
            input_blob.seek(0)
            df = pd.read_csv(input_blob)

        return df

    def upload_blob_zip(
        self, input_stream: bytes, container: str, blob_name: str
    ) -> None:
        """
        Description
        --
        Upload zip file to Azure Blob Storage

        Parameters
        --
        :param input_stream : input file bytes
        :param container : container name in Azure Blob Storage
        :param blob_name : blob name in container

        """
        # Get Azure Blob Storage client
        blob_client = self.service_client.get_blob_client(
            container=container, blob=blob_name
        )
        # Upload zip files into Azure Blob Storage
        blob_client.upload_blob(input_stream, blob_type="BlockBlob")


class OpQueuesHandler(object):
    """
    Initialazing Azure Queue Service client
    """

    def __init__(self, queue_name: str):
        # Defining class parametrs
        self.queue_name = queue_name

        current_path = Path().resolve()

        with open((os.path.join(current_path, "configuration.yml")), "r") as stream:
            config = yaml.safe_load(stream=stream)

        self.conn_str = config["AZURE_STORAGE"]["conn_string"]
        self.service_client = QueueClient.from_connection_string(
            conn_str=self.conn_str,
            queue_name=self.queue_name,
            message_encode_policy=BinaryBase64EncodePolicy(),
            message_decode_policy=BinaryBase64DecodePolicy(),
        )

    def insert_queque_msg(self, message: str) -> None:
        """
        Description
        --
        The method send message to Queque

        Parameters
        --
        :param message : queque message
        """

        self.service_client.send_message(message.encode(), visibility_timeout=10)

    def get_messages(self) -> list:
        """
        Description
        --
        The method receives all messages from Queque

        Output
        --
        messages : list of messages received form Queque
        """

        messages = self.service_client.receive_messages(visibility_timeout=10)

        return messages

    def delete_messages(self, message: QueueClient) -> None:
        """
        Description
        --
        The method delete messages from Azure Queque

        Parameters
        --
        message : message that will be deleted from Queque
        """
        self.service_client.delete_message(message=message)
