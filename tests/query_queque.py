from src.azure_api import OpQueuesHandler

# from src.utils import insert_dates

# # def read_msg():
# #     queue_name='dates-csv-messages'
# #     client = OpQueuesHandler()
# #     messages = client.get_messages(queue_name=queue_name)

# #     for m in messages:
# #         print(m.content)
# #         #  if m.content == '20230716_61.csv':
# #         #       client.delete_messages(queue_name, m)

# # def read_by_date():

# #     queue_name='dates-csv-messages'
# #     client = OpQueuesHandler()
# #     messages = client.get_messages(queue_name=queue_name)

# #     msg_content = []
# #     for m in messages:
# #         print(m.content)
# #         # print(m)
# #         # if m.content[0:9] == '20230716_':
# #         #     msg_content.append(m.content)

# #     print(msg_content)
# #     if len(msg_content) == 0:

# #         msg = client.get_messages('dates-to-insert')
# #         for ms in msg:
# #             if ms.content == '2023/07/16':
# #                 print(ms)


def mess():
    queue_client = OpQueuesHandler("test-trigger")
    # Create blob name
    # blob_name = myblob.name.split("/")[1]
    queue_client.insert_queque_msg("message")


if __name__ == "__main__":
    mess()
