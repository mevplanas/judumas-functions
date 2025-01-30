# Import necessary libraries
import azure.functions as func
from db import connection, models
import os
from src import azure_api
from blobhand.blob.reader import BlobServiceClientReader
from app import app as fastapi_app
# Create function app
app = func.AsgiFunctionApp(app=fastapi_app, http_auth_level=func.AuthLevel.ANONYMOUS)


# Create blob trigger function
@app.blob_trigger(
    arg_name="blob", path="strava/{name}.zip", connection="AzureConnectionString"
)
def strava_zip_blob_trigger(blob: func.InputStream):

    # Create file name from InputStream
    file_name = blob.name
    file_name = file_name.split("/")
    file_name = file_name[1:]
    blob_name = '/'.join(file_name)

    # Get connection string and queue name from environment variables
    conn_str = os.environ["AzureConnectionString"]
    queue = os.environ["QueueName"]

    # Create instance of OpQueuesHandler class
    queues_handler = azure_api.OpQueuesHandler(queue_name=queue, conn_str=conn_str)
    # Insert message to queue
    queues_handler.insert_queque_msg(blob_name)


@app.function_name("adf-read-mobility-data")
@app.route("adf/read/dates")
def read_mobility_dates(req: func.HttpRequest):

    # Get enironment variables
    conn_str = os.environ["JudumasConnectionString"]
    mssql_user = os.environ["MSSQL_USER"]
    mssql_password = os.environ["MSSQL_PASSWORD"]
    mssql_server = os.environ["MSSQL_SERVER"]
    mssql_database = os.environ["MSSQL_DATABASE"]
    mssql_driver = os.environ["MSSQL_DRIVER"]

    # Get connection string and queue name from environment variables
    container_name = req.params.get("azcontainer")
    csv_file = req.params.get("csvfile")
    travel_type = req.params.get("traveltype")

    # Create instance of SQLServerConnection class
    conn = connection.SQLServerConnection(user=mssql_user, password=mssql_password, host=mssql_server, db_name=mssql_database, driver=mssql_driver)

    max_id = models.OpRoutesDates.get_max_id(conn)

    if max_id is None:
        max_id = 0
    else:
        max_id = max_id + 1

    blob_client = BlobServiceClientReader.from_connection_string(conn_str=conn_str)

    df = blob_client.csv_to_dataframe(container=container_name, blob_name=csv_file)

    df['OBJECTID'] = max_id + df.index

    insertion_data = df.to_dict(orient='records')

    for row in insertion_data:
        # Get date string and object id from request
        date_str = row.get('dates')

        ojbect_id = row.get('OBJECTID')
        # Check and add date to the table based on travel type
        if travel_type == 'drive':
            models.OpRoutesDates.check_and_add_drive_date(conn, date_str, ojbect_id)
        elif travel_type == 'bikes':
            models.OpRoutesDates.check_and_add_bikes_date(conn, date_str, ojbect_id)
        elif travel_type == 'public_transport':
            models.OpRoutesDates.check_and_add_public_transport_date(conn, date_str, ojbect_id)
        elif travel_type == 'pedestrians':
            models.OpRoutesDates.check_and_add_pedestrians_date(conn, date_str, ojbect_id)
        else:
            return func.HttpResponse("Invalid travel type", status_code=400)

    return func.HttpResponse("Data inserted successfully", status_code=200)


# @app.blob_trigger(
#     arg_name="blob", path="anon-data/{name}.zip", connection="OpAnonDataStorage"
# )
# def clean_databricks_job_trigger(blob: func.InputStream):
#     # Get connection string and queue name from environment variables
#     url = "https://adb-4183188713368390.10.azuredatabricks.net/api/2.0/jobs/run-now"

#     headers = {
#         "Authorization": f"Bearer {os.environ['databricks_token']}"}

#     blob_date = blob.name.split("_")[0]

#     body = {
#         "job_id": int(os.environ["cleaning_job_id"]),
#         "job_parameters": {"blob_date": f"{blob_date}"},
#     }

#     response = requests.post(url, headers=headers, json=body)

#     if response.status_code == 200:
#         print("Job started successfully")
#     else:
#         print("Job failed to start")


# @app.queue_trigger(
#     arg_name="msg", queue_name="op-cleaned-data", connection="databricks_storage"
# )
# def routing_databricks_job_trigger(msg: func.QueueMessage):
#     # # Get connection string and queue name from environment variables
#     conn_str = os.environ["databricks_storage"]
#     queue = os.environ["queue_name"]

#     # Get message content
#     msg_content = msg.get_body().decode('utf-8')

#     # Create instance of OpQueuesHandler class
#     queue_handler = azure_api.OpQueuesHandler(queue_name=queue, conn_str=conn_str)

#     url = "https://adb-4183188713368390.10.azuredatabricks.net/api/2.0/jobs/run-now"

#     headers = {
#         "Authorization": f"Bearer {os.environ['databricks_token']}"}

#     body = {
#         "job_id": int(os.environ["routing_job_id"]),
#           "job_parameters": {
#             "input_data": f"{msg_content}"
#     }}

#     response = requests.post(url, headers=headers, json=body)

#     if response.status_code == 200:
#         print("Job started successfully")
#         # Delete message from queue
#         queue_handler.delete_messages(msg)

    