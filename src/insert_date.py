import pandas as pd
from db import connection, models
import os
from tqdm import tqdm
from dotenv import load_dotenv

if os.path.exists(".env"):
    load_dotenv(".env", verbose=True)

def main():
    df = pd.read_csv(r"C:\Users\rokas.petravicius\Downloads\dates.txt")

    mssql_user = os.environ["MSSQL_USER"]
    mssql_password = os.environ["MSSQL_PASSWORD"]
    mssql_server = os.environ["MSSQL_SERVER"]
    mssql_database = os.environ["MSSQL_DATABASE"]
    mssql_driver = os.environ["MSSQL_DRIVER"]

    conn = connection.SQLServerConnection(user=mssql_user, password=mssql_password, host=mssql_server, db_name=mssql_database, driver=mssql_driver)

    max_id = models.OpRoutesDates.get_max_id(conn)
    if max_id is None:
        max_id = 0
    else:
        max_id = max_id + 1

    df['OBJECTID'] = max_id + df.index

    insertion_data = df.to_dict(orient='records')

    for row in tqdm(insertion_data, desc="Inserting dates", total=len(insertion_data)):
        # Get date string and object id from request
        date_str = row.get('dates')

        ojbect_id = row.get('OBJECTID')
        try:
            models.OpRoutesDates.check_and_add_drive(conn, date_str, ojbect_id)
        except Exception as e:
            print(e)
if __name__ == "__main__":
    main()
