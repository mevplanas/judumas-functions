from azure.storage.queue import (
    QueueClient,
    BinaryBase64EncodePolicy,
    BinaryBase64DecodePolicy,
)


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
