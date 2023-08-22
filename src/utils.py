# Import libraries
from datetime import datetime
import string
import random
import zipfile
import io
from src import arcgis
import yaml
import os
from pathlib import Path


def create_random(len_str: int) -> string:
    """
    Desctiption
    --
    The functions generate random string

    Parameters
    --
    :param len_str: number of characters in string

    Output
    --
    ran : generated random string value
    """
    # Generate random string with uppercase letters and numbers.
    ran = "".join(random.choices(string.ascii_uppercase + string.digits, k=len_str))
    return ran


def get_time():
    """
    Description
    --
    The functions returns today date and hour

    Ouput
    --
    @date_query: date string format in %Y%m%d
    @date_timestamp: datetime string format in %Y-%m-%d %H:%M:%S
    @hour: hour string in format %H
    @date_agol_query: date string format in %Y-%m-%d
    """

    # Checking if unix in seconds
    now = datetime.now()
    date_query = now.strftime(("%Y%m%d"))
    date_agol_query = now.strftime(("%Y-%m-%d"))
    date_timestamp = now.strftime(("%Y-%m-%d %H:%M:%S"))
    if now.hour < 11:
        hour = "08"
    else:
        hour = "15"

    return date_query, date_timestamp, hour, date_agol_query


def create_chunks(n: int, data: list):
    """
    Description
    --
    The function create list of lists (chunks).

    Parameters
    :param n : amount of chunks
    :param data : data

    Output
    --
    chunks : list of lists
    """
    chunks = [data[i * n : (i + 1) * n] for i in range((len(data) + n - 1) // n)]
    return chunks


def insert_dates(dates_queques_client, insert_queques_client) -> list:
    """
    Description
    --
    The function compares Queques and return not overlaping queques messages from
    dates_queques Azure Queque. Messages than converted to list of dictionaries for
    acceptable format for ArcGIS Rest Service addFeatures method.

    Parameters
    --
    :param dates_queques : Queque name
    :param insert_queques : Queque name

    Output
    --
    arcgis_dict : list of dictionaries for addFeatures method
    """

    # Get all messages from Queque client
    messages_dates = dates_queques_client.get_messages()
    messages_inserts = insert_queques_client.get_messages()

    # Get all message content and append it to inserts list
    inserts = []
    for msg_insert in messages_inserts:
        try:
            # Append content to list until '_' symbol
            inserts.append(str(msg_insert.content).split("_")[0])
        except Exception as e:
            print(e)

    # Get all message content and append it to dates list
    dates = []
    for msg_date in messages_dates:
        try:
            # Append content to list with replaced simbols
            dates.append(
                {"msg_id": msg_date.id, "date": str(msg_date.content).replace("/", "")}
            )
        except Exception as e:
            print(e)

    # Check if dates not overlaping
    # if not append to insert_into list

    insert_into = []
    for date in dates:
        if dates[date]["date"] not in inserts:
            insert_into.append(dates[date])
        else:
            pass

    # Append data to arcgis_dict list for data injection into ArcGIS Rest Service
    arcgis_dict = []
    for date in insert_into:
        # # Create dictionary for data injection
        # arcgis_dict.append({'attributes': {
        #     'date_str' : f"{date[0:4]}/{date[4:6]}/{date[6:8]}"
        # }})
        print(date)

    return arcgis_dict


def query_max():
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

    query = [
        {
            "statisticType": "max",
            "onStatisticField": "OBJECTID",
            "outStatisticFieldName": "max_obj_id",
        }
    ]

    url = "https://opencity.vplanas.lt/arcgis/rest/services/P_Judumas/Judumas_OP_data_update/FeatureServer/1"

    data = arc_conn.query_max_id(url, query)
    max_id = data["features"][0]["attributes"]["max_obj_id"]

    return max_id


def convert_entities(table_client, entities: list[dict]):
    entities_converted = []
    for el in entities:
        entity = table_client.create_entity(el)
        entities_converted.append(entity)

    return entities_converted


def zip_to_list(bytes: bytes, columns: list):
    final_data = []
    zipped = zipfile.ZipFile(io.BytesIO(bytes))
    files_list = zipped.namelist()
    # Iterate through all files inside zip file
    # and add all recrods to final_data list
    for file in files_list:
        data = zipped.read(file)
        rows = str(data, "utf-8")
        lst = rows.split()
        for el in lst:
            elements = el.split(",")
            elements.append(file)
            elements_dict = dict(zip(columns, elements))
            final_data.append(elements_dict)

    return final_data
