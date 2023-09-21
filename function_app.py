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
import io
import zipfile
from urllib import request
import io
import re
import fiona
import geopandas as gpd

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
                }
            )

        # Create dataframe from remaped data
        df_op = pd.DataFrame(entities)
        # Merge OSM network with remaped data
        data = df_op.merge(df_osm, on="osm_id", how="inner")

        max_id = utils.query_max()

        data["OBJECTID"] = data.index + max_id + 1
        # Convert data to list of dictionaries
        entities = data.to_dict(orient="records")
        # Remaping data for Azure Table insertion
        # (IT'S NOT NECCESSERY SO SHOULD BE REMOVE)
        entities_converted = utils.convert_entities(
            table_client=data_handler, entities=entities
        )
        # Spliting all data into chunks
        chunks = utils.create_chunks(n=50000, data=entities_converted)
        i = 0
        # Upload data as csv files into Azure Blob Storage
        for chunk in tqdm(chunks, total=len(chunks), desc="Inserting data"):
            blob_handler.upload_blob_csv(
                "op-test-csv", name=f"{op_date}_{i}", data=chunk
            )
            i += 1

    else:
        logging.info(f"{myblob.name} is not a zip file")


# @app.blob_trigger(arg_name="myblob", path="op-test-csv", connection="judumas_STORAGE")
# def csv_trigger(myblob: func.InputStream):
#     """
#     Description
#     --
#     Detect new csv file in Azure Storage and inserts data into
#     Mobility data SQL SERVER thorough ArcGIS REST API addFeatures method

#     Parameters
#     --
#     :param myblob : blob input stream
#     """
#     queue_client = az.OpQueuesHandler("test-trigger")
#     # Create blob name
#     blob_name = myblob.name.split("/")[1]
#     queue_client.insert_queque_msg(f"{blob_name}")


# @app.route(route="zipuploader", auth_level=func.AuthLevel.FUNCTION)
# def zip_uploader(req: func.HttpRequest) -> func.HttpResponse:
#     """
#     Description
#     --
#     The function creates http request for uplading zip file into blob storage.
#     Path to api endpoint http://{host}/api/zipuploader
#     Also insert date value into JUDUMAS.OP_ROUTES_DATES table

#     Parameters
#     --
#     :param req: http request
#     """

#     # Initializing current path
#     current_path = Path().resolve()

#     # Open confiuration file as a stream
#     with open((os.path.join(current_path, "configuration.yml")), "r") as stream:
#         config = yaml.safe_load(stream=stream)

#         # Loading Azure Storage credentials
#     arc_username = config["OPENCITY_PORTAL"]["user"]
#     arc_password = config["OPENCITY_PORTAL"]["password"]
#     arc_portal = config["OPENCITY_PORTAL"]["portal_link"]

#     # Initialazing ArcGIS
#     arc_conn = arcgis.ArcGisRestConnector(
#         username=arc_username, password=arc_password, portal=arc_portal
#     )

#     # Load file from http request into Azure Blob Storage
#     for input_file in req.files.values():
#         filename = input_file.filename
#         if filename.endswith(".zip"):
#             filename = input_file.filename
#             content = input_file.stream.read()
#             blob_handler = az.OpBlobHandler()
#             blob_handler.upload_blob_zip(
#                 content, container="op-test", blob_name=filename
#             )
#             # az_queues = az.OpQueuesHandler('dates-to-insert')
#             # Create datetime object for created_at and updated_at fields
#             now = utils.get_time()[1]
#             datetime_object = (
#                 time.mktime(datetime.strptime(now, "%Y-%m-%d %H:%M:%S").timetuple())
#                 * 1000
#             )
#             # Create dictionary with data
#             format_date = [
#                 {
#                     "date_str": f"{filename[0:4]}/{filename[4:6]}/{filename[6:8]}",
#                     "created_at": int(datetime_object),
#                     "updated_at": int(datetime_object),
#                 }
#             ]
#             # Insert data into SQL Server through ArcGIS REST API
#             url = "https://opencity.vplanas.lt/arcgis/rest/services/P_Judumas/Judumas_OP_data_update/FeatureServer/10"
#             values = arc_conn.data_handler(format_date)
#             arc_conn.insert_records(service_url=url, features=values)

#     return func.HttpResponse(f"File {str(filename)} uploaded")


# @app.queue_trigger(
#     arg_name="azqueue", queue_name="test-trigger", connection="judumas_STORAGE"
# )
# def queue_trigger(azqueue: func.QueueMessage):
#     message = azqueue.get_body().decode("utf-8")
#     headers = {
#         "x-functions-key": "bcqSfNL32gvz-JPeXIdeGgsOjLAtfV90SjDVyyW4XJJqAzFuo88QMw=="
#     }
#     response = requests.get(
#         r"https://judumas.azurewebsites.net/api/insert_data",
#         params={"filename": message},
#         headers=headers,
#     )
#     logging.info(response.content)
#     response_dict = ast.literal_eval(response.content.decode("utf-8"))
#     try:
#         if response_dict["error"]["code"] == 500:
#             raise AssertionError
#         else:
#             pass
#     except KeyError:
#         logging.info("Error key not exists, due response was suscessful")


# @app.route(route="insert_data", auth_level=func.AuthLevel.FUNCTION)
# def insert_data(req: func.HttpRequest) -> func.HttpResponse:
#     filename = req.params.get("filename")
#     if filename:
#         # Initializing current path
#         current_path = Path().resolve()

#         # Open confiuration file as a stream
#         with open((os.path.join(current_path, "configuration.yml")), "r") as stream:
#             config = yaml.safe_load(stream=stream)

#         # Loading Azure Storage credentials
#         arc_username = config["OPENCITY_PORTAL"]["user"]
#         arc_password = config["OPENCITY_PORTAL"]["password"]
#         arc_portal = config["OPENCITY_PORTAL"]["portal_link"]

#         # Initialazing Azure Storage mobility data handler
#         blob_handler = az.OpBlobHandler()

#         # Initialazing ArcGIS
#         arc_conn = arcgis.ArcGisRestConnector(
#             username=arc_username, password=arc_password, portal=arc_portal
#         )
#         # Load csv files and convert it to dataframe
#         df = blob_handler.read_csv("op-test-csv", filename)

#         # Create neccessery fields
#         df["created_at"] = utils.get_time()[1]
#         df["updated_at"] = utils.get_time()[1]
#         df["op_date"] = df["op_date"].astype(str)
#         df["datetime"] = df.apply(
#             lambda row: f"{row['op_date'][0:4]}-{row['op_date'][4:6]}-{row['op_date'][6:8]} {row['op_hour']}:00:00",
#             axis=1,
#         )
#         # Convert string timestamp into unix timestamp
#         df["datetime"] = df.apply(
#             lambda row: int(
#                 time.mktime(
#                     datetime.strptime(row["datetime"], "%Y-%m-%d %H:%M:%S").timetuple()
#                 )
#             )
#             * 1000,
#             axis=1,
#         )
#         df["created_at"] = df.apply(
#             lambda row: int(
#                 time.mktime(
#                     datetime.strptime(
#                         row["created_at"], "%Y-%m-%d %H:%M:%S"
#                     ).timetuple()
#                 )
#             )
#             * 1000,
#             axis=1,
#         )
#         df["updated_at"] = df.apply(
#             lambda row: int(
#                 time.mktime(
#                     datetime.strptime(
#                         row["updated_at"], "%Y-%m-%d %H:%M:%S"
#                     ).timetuple()
#                 )
#             )
#             * 1000,
#             axis=1,
#         )

#         df["parent_global_id"] = df["GlobalID"]
#         df["parent_global_id"] = df.apply(
#             lambda row: f"{{{row['parent_global_id']}}}", axis=1
#         )
#         # Leave only neccessery fields in dataframe
#         df = df[
#             [
#                 "OBJECTID",
#                 "osm_id",
#                 "usage",
#                 "parent_global_id",
#                 "datetime",
#                 "created_at",
#                 "updated_at",
#             ]
#         ]

#         # Convert dataframe into list of dictionaries
#         data = df.to_dict(orient="records")
#         # Prepering data for ArcGIS REST addFeatures method
#         data_converted = arcgis.ArcGisRestConnector.data_handler(data=data)
#         url = "https://opencity.vplanas.lt/arcgis/rest/services/P_Judumas/Judumas_OP_data_update/FeatureServer/9"
#         # Insert all data into database through ArcGIS REST Service
#         response = arc_conn.insert_records(service_url=url, features=data_converted)
#         return func.HttpResponse(f"{response}")


@app.route(route="strava_uploader", auth_level=func.AuthLevel.FUNCTION)
def strava_uploader(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")

    # Load file from http request into Azure Blob Storage
    for input_file in req.files.values():
        logging.info(input_file)

        filename = input_file.filename
        if filename.endswith(".zip"):
            filename = input_file.filename
            content = input_file.stream.read()
            blob_handler = az.OpBlobHandler()
            if filename.endswith("_ride.zip"):
                blob_handler.upload_blob_zip(
                    content, container="strava/ride", blob_name=filename
                )
            if filename.endswith("_ped.zip"):
                blob_handler.upload_blob_zip(
                    content, container="strava/ped", blob_name=filename
                )
            logging.info(filename)
            return func.HttpResponse(f"File {str(filename)} uploaded")


@app.blob_trigger(arg_name="myblob", path="strava", connection="judumas_STORAGE")
def strava_mapper(myblob: func.InputStream):
    logging.info(f"Loading {myblob.name}")

    if myblob.name.endswith(".zip"):
        name = myblob.name.split("/")[2]
        name = name.split(".")[0]
        # Read zip file as bytes
        data_as_bytes = myblob.read()
        _bytes = io.BytesIO(data_as_bytes)

        # Loading shapefile
        with fiona.BytesCollection(_bytes.read()) as src:
            crs = src.crs
            df_shape = gpd.GeoDataFrame.from_features(src, crs=crs)

        # Loading csv file
        zipped = zipfile.ZipFile(_bytes)
        files_list = zipped.namelist()
        unzipped = []
        substring = '"'
        for file in files_list:
            if file.endswith(".csv"):
                data = zipped.read(file)
                rows = str(data, "utf-8")
                patern = substring + "(.+?)" + substring
                str_found = re.search(patern, rows).group(1)

                data_list = rows.split()
                columns = data_list[0].split(",")
                for el in data_list[1:]:
                    el = re.sub(patern, str_found.replace(",", ""), el)
                    elements = el.split(",")
                    elements_dict = dict(zip(columns, elements))
                    unzipped.append(elements_dict)

        df_csv = pd.DataFrame(data=unzipped)
        df_csv["edge_uid"] = df_csv["edge_uid"].astype("int64")
        df_shape["edgeUID"] = df_shape["edgeUID"].astype("int64")
        # Merge csv and shape
        df = pd.merge(
            df_csv, df_shape, left_on="edge_uid", right_on="edgeUID", how="left"
        ).drop("edgeUID", axis=1)

        data_dict = df.to_dict(orient="records")
        blob_handler = az.OpBlobHandler()

        # Spliting all data into chunks
        chunks = utils.create_chunks(n=50000, data=data_dict)
        i = 0
        # Upload data as csv files into Azure Blob Storage
        for chunk in tqdm(chunks, total=len(chunks), desc="Inserting data"):
            blob_handler.upload_blob_csv("strava-csv", name=f"{name}_{i}", data=chunk)
            i += 1
