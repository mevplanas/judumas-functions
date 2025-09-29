from azure.storage.queue import (
    QueueClient,
    BinaryBase64EncodePolicy,
    BinaryBase64DecodePolicy,
)
import datetime
from azure.data.tables import TableServiceClient, EntityProperty, EdmType
import uuid
import logging


class OpQueuesHandler(object):
    """
    Initialazing Azure Queue Service client
    """

    def __init__(self, queue_name: str, conn_str: str):

        # Defining class parametrs
        self.queue_name = queue_name
        self.conn_str = conn_str

        # Connecting to Azure Queque Service client
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


class AzTableHandler(object):
    """
    Initialazing Azure Table Service client
    """

    def __init__(self, conn_str: str):

        # Defining class parametrs
        self.conn_str = conn_str

        # Connecting to Azure Table Service client
        self.service_client = TableServiceClient.from_connection_string(conn_str=self.conn_str)

    def insert_entities(self, table_name: str, entities: list) -> None:
        """
        Description
        --
        The method insert entities to Azure Table

        Parameters
        --
        :param table_name : name of the table where entities will be inserted
        :param entities : list of entities that will be inserted to table
        """

        # Create table if not exists
        table_client = self.service_client.get_table_client(table_name)

        # current date

        current_date = datetime.datetime.utcnow().strftime("%Y%m%d")

        for entity in entities:
            entity = self.dictionary_to_entity(entity)
            entity["startTime"] = EntityProperty(entity["startTime"], EdmType.INT64)
            entity["endTime"] = EntityProperty(entity["endTime"], EdmType.INT64)
            entity["pubMillis"] = EntityProperty(entity["pubMillis"], EdmType.INT64)
            entity["PartitionKey"] = current_date
            entity["RowKey"] = str(uuid.uuid4())
            table_client.create_entity(entity=entity)

        logging.info(f"Inserted {len(entities)} entities to table {table_name}")

    @staticmethod
    def dictionary_to_entity(entity: dict) -> dict:
        """
        Description
        --
        The method converts dictionary to entity

        Parameters
        --
        :param entity : dictionary that will be converted to entity

        Output
        --
        entity : entity
        """
        for key, value in entity.items():
            if isinstance(value, list) or isinstance(value, dict):
                # Convert list or dictionary to string
                entity[key] = str(value)
        return entity
