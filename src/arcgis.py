# Importing libraries
import json
from urllib import parse, request


class ArcGisRestConnector(object):
    """
    Description
    -
    Connector to ArcGIS Rest services

    """

    def __init__(self, username: str, password: str, portal: str):
        """
        Parameters
        -
        :param username : ArcGIS Portal user username
        :param password : ArcGIS Portal user password
        :param portal : url link to ArcGIS Enterprise Portal

        """
        self.username = username
        self.password = password
        self.portal = portal

    def _generate_token(self) -> str:
        """
        Description
        -
        The method creates temporal token to access ArcGIS Rest services

        Output
        -
        token : str
                token to access ArcGIS Rest services
        """
        params = {
            "f": "pjson",
            "username": self.username,
            "password": self.password,
            "referer": self.portal,
        }

        token_url = f"{self.portal}/portal/sharing/rest/generateToken"
        data = parse.urlencode(query=params).encode("utf-8")
        req = request.Request(token_url, data)
        response = request.urlopen(req)
        json_response = json.loads(response.read())
        token = json_response["token"]
        return token

    def query_fc(self, service_url, query) -> dict:
        """
        Description
        -
        The method queries defined ArcGIS Rest Service

        Parameters
        -
        :param service_url : link to ArcGIS Rest Service
        :param query : ArcGIS Rest Service 'where' clause query

        Documentation to format proper ArcGIS Rest query can be found here
        https://developers.arcgis.com/rest/services-reference/enterprise/query-feature-service-layer-.html
        For now it only accepts 'where' clause request parameter.

        Output
        -
        json_return : response from url request

        """
        token = self._generate_token()

        params = {"where": query, "outFields": "*", "f": "json", "token": token}

        url = f"{service_url}/query?"
        encode_params = parse.urlencode(params).encode("utf-8")
        response = request.urlopen(url, encode_params)
        data_response = response.read()
        json_return = json.loads(data_response)
        return json_return

    def insert_records(self, service_url: str, features: list) -> None:
        """
        Description
        -
        Insert records into ArcGIS Rest service using addFeatures method

        Parameters
        -
        :param service_url : url to ArcGIS Rest service
        :param features : insert elements\n
                Example of features :
                        features = [
                                        {
                                        "attributes": {
                                                "date_str": "2015/04/01",
                                                }
                                        },
                                        {
                                        "attributes": {
                                                "date_str": "2015/03/31",
                                                }
                                        }
                                        ]

        """
        token = self._generate_token()
        payload = {"f": "json", "features": features, "token": token}
        url = f"{service_url}/addFeatures"

        result = request.urlopen(url, parse.urlencode(payload).encode("utf-8")).read()
        json_result = json.loads(result)

        return json_result

    def query_max_id(self, service_url: str, query):
        token = self._generate_token()
        payload = {"f": "json", "token": token, "outStatistics": query}
        url = f"{service_url}/query?"

        result = request.urlopen(url, parse.urlencode(payload).encode("utf-8")).read()
        json_result = json.loads(result)

        return json_result

    @classmethod
    def data_handler(cls, data: list) -> list:
        """
        Description
        -
        The method converts list of dictionaries into ArcGIS Rest service acceptable list of dictionaries.

        Parameters
        -
        :param data: list of dictionaries

                Example of input data:

                [{'osm_id': 3412414, 'usage': 41, 'GlobalID': '2E8E6C0E-C355-4EDA-B95D-CCADCD38DF85',\n
                'datetime': 1689483600, 'created_at': 1690200956, 'updated_at': 1690200956}, \n
                {'osm_id': 3412414, 'usage': 59, 'GlobalID': '2E8E6C0E-C355-4EDA-B95D-CCADCD38DF85', \n
                'datetime': 1689487200, 'created_at': 1690200956, 'updated_at': 1690200956},\n
                {'osm_id': 3412414, 'usage': 93, 'GlobalID': '2E8E6C0E-C355-4EDA-B95D-CCADCD38DF85', \n
                'datetime': 1689490800, 'created_at': 1690200956, 'updated_at': 1690200956}]\n

        Output
        -
        reshaped_data: list
                converted list of dictionaries

                Example of output data:

                [{'attributes':{'osm_id': 3412414, 'usage': 41, 'GlobalID': '2E8E6C0E-C355-4EDA-B95D-CCADCD38DF85',\n
                'datetime': 1689483600, 'created_at': 1690200956, 'updated_at': 1690200956}}, \n
                {'attributes':{'osm_id': 3412414, 'usage': 59, 'GlobalID': '2E8E6C0E-C355-4EDA-B95D-CCADCD38DF85', \n
                'datetime': 1689487200, 'created_at': 1690200956, 'updated_at': 1690200956}},\n
                {'attributes':{'osm_id': 3412414, 'usage': 93, 'GlobalID': '2E8E6C0E-C355-4EDA-B95D-CCADCD38DF85', \n
                'datetime': 1689490800, 'created_at': 1690200956, 'updated_at': 1690200956}}]\n


        """
        reshaperd_data = []
        if len(data) == 0:
            reshaperd_data.append({"attributes": data[0]})
        else:
            for el in data:
                new_dict = {"attributes": el}
                reshaperd_data.append(new_dict)

        return reshaperd_data
