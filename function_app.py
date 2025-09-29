# Import necessary libraries
import azure.functions as func
import os
from src import azure_api
import requests
import logging


def add_timestamp(data, start_time, end_time):

    if data:
        for row in data:
            row["startTime"] = start_time
            row["endTime"] = end_time

    return data


# Create FunctionApp instance
app = func.FunctionApp()


# Create blob trigger function
@app.blob_trigger(arg_name="blob", path="strava/{name}.zip", connection="AzureConnectionString")
def strava_zip_blob_trigger(blob: func.InputStream):

    # Create file name from InputStream
    file_name = blob.name
    file_name = file_name.split("/")
    file_name = file_name[1:]
    blob_name = "/".join(file_name)

    # Get connection string and queue name from environment variables
    conn_str = os.environ["AzureConnectionString"]
    queue = os.environ["QueueName"]

    # Create instance of OpQueuesHandler class
    queues_handler = azure_api.OpQueuesHandler(queue_name=queue, conn_str=conn_str)
    # Insert message to queue
    queues_handler.insert_queque_msg(blob_name)


@app.timer_trigger(schedule="0 */1 * * * *", arg_name="myTimer", run_on_startup=True, use_monitor=False)
def waze_ingest(myTimer: func.TimerRequest) -> None:

    # Get Waze API endpoint from environment variables
    waze_api_endpoint = os.environ["WazeApiEndpoint"]
    # Make GET request to Waze API endpoint

    response = requests.get(waze_api_endpoint)

    if response.status_code == 200:
        data = response.json()

        waze_allerts = data.get("alerts", [])
        waze_jams = data.get("jams", [])

        start_time = data.get("startTimeMillis", 0)
        end_time = data.get("endTimeMillis", 0)

        waze_allerts = add_timestamp(waze_allerts, start_time, end_time)
        waze_jams = add_timestamp(waze_jams, start_time, end_time)

        # Get connection string and table names from environment variables
        conn_str = os.environ["WazeStorageConnectionString"]
        alerts_table = os.environ["WazeAlertsTableName"]
        jams_table = os.environ["WazeJamsTableName"]

        # Create instance of OpTablesHandler class
        table_handler = azure_api.AzTableHandler(conn_str=conn_str)

        if waze_allerts:
            table_handler.insert_entities(table_name=alerts_table, entities=waze_allerts)

        if waze_jams:
            table_handler.insert_entities(table_name=jams_table, entities=waze_jams)

    if myTimer.past_due:
        logging.info("The timer is past due!")

    logging.info("Python timer trigger function executed.")
