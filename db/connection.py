from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os
from datetime import datetime
from multiprocessing import Pool
import numpy as np
import pyodbc

import pandas as pd

CURR_PATH = os.path.dirname(os.path.abspath(__file__))
ENV_FILE = "../sqlserv-cred.env"


class SQLServerConnection(object):
    """
    Connection configuration and connection instance to MSSQL.
    """

    def __init__(self) -> None:
        load_dotenv(dotenv_path=os.path.join(CURR_PATH, ENV_FILE))
        # Loading MS SQL Server database credentials
        self.user = os.environ.get("SQLSERVER_USER")
        self.pw = os.environ.get("SQLSERVER_PASSWORD")
        self.host = os.environ.get("SQLSERVER_HOST")
        self.db = os.environ.get("SQLSERVER_DB")
        self.driver = os.environ.get("SQLSERVER_DRIVER")

    def _con_alchemy(self):
        """
        Connection with MSSQL using SQL Alchemy.
        """

        conn = create_engine(
            f"mssql+pyodbc://{self.user}:{self.pw}@{self.host}\
        /{self.db}?driver={self.driver}&NeedODBCTypesOnly=1",
            echo=False,
            pool_pre_ping=True,
            pool_recycle=3600,
        )

        return conn

    def _con_pyodbc(self) -> pyodbc.Connection:
        """
        Connection object with MSSQL using PyODBC.
        """

        conn = pyodbc.connect(
            driver="{ODBC Driver 17 for SQL Server}",
            server=self.host,
            database=self.db,
            uid=self.user,
            pwd=self.pw,
        )

        return conn

    def create_session(self):
        """
        Session object.
        """

        Session = sessionmaker(bind=self._con_alchemy())
        session = Session()

        return session

    def read_data(self, query: str) -> pd.DataFrame:
        """
        Query data from MSSQL Table and create Dataframe

        Parameters
        query: SQL Query
        """

        _conn = self._con_alchemy()

        sql = text(f"{query}")
        df = pd.read_sql(sql, _conn)

        return df

    def _delete_all(self):
        """
        Delete all values from table.
        """

        _query = f"DELETE FROM {self.table}"
        _conn = self._con_pyodbc()
        _cur = _conn.cursor()
        _cur.execute(_query)
        _cur.commit()
        _cur.close()

    def _multi_process(self, _df):
        """
        Insert values into table.
        """

        start_time = datetime.now()
        _conn = self._con_pyodbc()
        _cur = _conn.cursor()
        _cur.fast_executemany = False
        _cur.executemany(self.insert_query, _df.values.tolist())
        _cur.commit()
        _cur.close()
        end_time = datetime.now()
        print(f"{len(_df)} rows inserted into database")
        print(end_time - start_time)

    def update_heatmap(self, table: str, df: pd.DataFrame, delete: bool):
        """
        Function delete all values from table and write new rows from dataframe
        """

        self.table = table
        self.df = df

        fields = df.columns.to_list()
        fields.remove("Shape")
        re_fields = ["Shape"]
        re_fields.extend(fields)
        insert_simbols = []
        for el in fields:
            insert_simbols.append("?")

        insert_query = f"""INSERT INTO {table} ({', '.join(re_fields)}) \
            VALUES(geometry::STGeomFromText(?, 4326), {', '.join(insert_simbols)});"""

        self.insert_query = insert_query

        if delete == True:
            self._delete_all()
        else:
            pass

        if len(df) > 200:
            divider = df.index.max() / 200
            df_list = np.array_split(df, divider)

            with Pool() as pool:
                pool.map(self._multi_process, df_list)

        else:
            self._multi_process(df)
