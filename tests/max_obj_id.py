from src import arcgis
import yaml
import os
from pathlib import Path


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
