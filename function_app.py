# Import necessary libraries
import azure.functions as func
from src import azure_api
import os

# Create function app
app = func.FunctionApp()

# Create blob trigger function
@app.blob_trigger(arg_name="myblob",path='strava/{name}.zip', connection='AzureWebJobsStorage')
def my_blob_trigger(blob: func.InputStream):
        
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