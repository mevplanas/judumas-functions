from src import azure_api as az
import pandas as pd
import time


def osm_max_log() -> int:
    data_handler = az.OpTableHandler()

    logs_query = "PartitionKey gt ''"
    data = data_handler.query_entities(table_name="OsmUpdateLogs", query=logs_query)

    df = pd.DataFrame(data=data)

    max_value = df["PartitionKey"].max()

    return max_value


def get_az_table_osm(max_partition):
    data_handler = az.OpTableHandler()
    logs_query = f"PartitionKey eq '{max_partition}'"
    data = data_handler.query_entities(table_name="OsmNetwork", query=logs_query)
    df = pd.DataFrame(data=data)

    df = df[["osm_id", "GlobalID"]]

    return df


if __name__ == "__main__":
    start_time = time.time()
    max_date = osm_max_log()
    df = get_az_table_osm(max_partition=max_date)

    print(df)
    end_time = time.time()
    print(end_time - start_time)
