import json
from urllib import parse, request
from src.utils import get_time
import pandas as pd
from src import azure_api as az
import time


def read_all():
    # Initialazing Azure Storage mobility data handler
    blob_handler = az.OpBlobHandler()
    numbers = list(range(147))
    dfs = []
    for file in numbers:
        filename = f"20230604_{file}.csv"
        df = blob_handler.read_csv("op-test-csv", filename)
        # dfs.append(df)

    # data = pd.concat(dfs, ignore_index=True)
    # print(len(data))


if __name__ == "__main__":
    start = time.time()
    read_all()
    end = time.time()
    print(end - start)
