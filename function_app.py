# Importing libraries
import azure.functions as func
import os
from pathlib import Path
import yaml
import logging
import pandas as pd
from tqdm import tqdm
from datetime import datetime
import time
import requests
import ast

# Import custom modules
from src import azure_api as az
from src import arcgis
from src import utils

# Initialazing azure function app
app = func.FunctionApp()


# Blob trigger decorator
@app.blob_trigger(
    arg_name="myblob",
    path="op-test-adf",
    connection="judumas_STORAGE",
    kwargs={},
)
def op_mapper(myblob: func.InputStream):
    """
    Description
    --
    Function detect new zip file in 'op-test' Azure Blob Storage
    and converts it to multiple csv files. Files than uploading into 'op-test-cs' storage.
    Also Queque messages are inserted for further processing.

    Parameters
    --
    :param myblob : blob input stream
    """

    # Checks if input file is zip file
    if myblob.name.endswith(".zip"):
        name = myblob.name.split("/")[1]
        name = name.split(".")[0]
        # Read zip file as bytes
        data_as_bytes = myblob.read()
        # Intialazing azure clients
        data_handler = az.OpTableHandler()
        blob_handler = az.OpBlobHandler()
        # Query OsmUpdateLogs table where info with last update date is stored
        logs_query = "PartitionKey gt ''"
        max_log = data_handler.query_entities(
            table_name="OsmUpdateLogs", query=logs_query
        )
        # Create dataframe from query results
        df = pd.DataFrame(data=max_log)
        # Get max PartitionKey value which is also max date value
        max_value = df["PartitionKey"].max()

        # Query OsmNetwork table and retrieve Open Street Map network
        logs_query = f"PartitionKey eq '{max_value}'"
        data = data_handler.query_entities(table_name="OsmNetwork", query=logs_query)
        # Create dataframe from retrieved data
        df = pd.DataFrame(data=data)

        # Leave only neccessary fields
        df_osm = df[["osm_id", "GlobalID"]]

        # Define needed columns from zip file data
        columns = ["osm_id", "usage", "filename"]

        final_data = utils.zip_to_list(data_as_bytes, columns)
        logging.info(len(final_data))

        # Remaping data
        entities = []
        for el in final_data:
            _date = el["filename"].split("/")[1]
            op_date = _date.split("_")[0]
            op_hour = _date.split("_")[1]
            entities.append(
                {
                    "osm_id": el["osm_id"],
                    "usage": el["usage"],
                    "op_date": op_date,
                    "op_hour": op_hour,
                    "filename": el["filename"],
                    "created_at": utils.get_time()[1],
                    "updated_at": utils.get_time()[1],
                    "datetime": f"{op_date[0:4]}-{op_date[4:6]}-{op_date[6:8]} {op_hour}:00:00",
                }
            )

        # Create dataframe from remaped data
        df_op = pd.DataFrame(entities)
        # Merge OSM network with remaped data
        data = df_op.merge(df_osm, on="osm_id", how="inner")

        # Convert data to list of dictionaries
        entities = data.to_dict(orient="records")
        # Upload data as csv files into Azure Blob Storage
        # for chunk in tqdm(chunks, total=len(chunks), desc="Inserting data"):
        blob_handler.upload_blob_csv("op-test-csv-adf", name=op_date, data=entities)
        # i += 1

    else:
        logging.info(f"{myblob.name} is not a zip file")


@app.route(route="zipuploader", auth_level=func.AuthLevel.FUNCTION)
def zip_uploader(req: func.HttpRequest) -> func.HttpResponse:
    """
    Description
    --
    The function creates http request for uplading zip file into blob storage.
    Path to api endpoint http://{host}/api/zipuploader
    Also insert date value into JUDUMAS.OP_ROUTES_DATES table

    Parameters
    --
    :param req: http request
    """

    # Initializing current path
    current_path = Path().resolve()

    # Open confiuration file as a stream
    with open((os.path.join(current_path, "configuration.yml")), "r") as stream:
        config = yaml.safe_load(stream=stream)

        # Loading Azure Storage credentials
    arc_username = config["OPENCITY_PORTAL"]["user"]
    arc_password = config["OPENCITY_PORTAL"]["password"]
    arc_portal = config["OPENCITY_PORTAL"]["portal_link"]

    # Initialazing ArcGIS
    arc_conn = arcgis.ArcGisRestConnector(
        username=arc_username, password=arc_password, portal=arc_portal
    )

    # Load file from http request into Azure Blob Storage
    for input_file in req.files.values():
        filename = input_file.filename
        if filename.endswith(".zip"):
            filename = input_file.filename
            content = input_file.stream.read()
            blob_handler = az.OpBlobHandler()
            blob_handler.upload_blob_zip(
                content, container="op-test", blob_name=filename
            )
            # az_queues = az.OpQueuesHandler('dates-to-insert')
            # Create datetime object for created_at and updated_at fields
            now = utils.get_time()[1]
            datetime_object = (
                time.mktime(datetime.strptime(now, "%Y-%m-%d %H:%M:%S").timetuple())
                * 1000
            )
            # Create dictionary with data
            format_date = [
                {
                    "date_str": f"{filename[0:4]}/{filename[4:6]}/{filename[6:8]}",
                    "created_at": int(datetime_object),
                    "updated_at": int(datetime_object),
                }
            ]
            # Insert data into SQL Server through ArcGIS REST API
            url = "https://opencity.vplanas.lt/arcgis/rest/services/P_Judumas/Judumas_OP_data_update/FeatureServer/10"
            values = arc_conn.data_handler(format_date)
            arc_conn.insert_records(service_url=url, features=values)

    return func.HttpResponse(f"File {str(filename)} uploaded")
