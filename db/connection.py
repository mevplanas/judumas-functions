import pyodbc

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class SQLServerConnection(object):
    """
    Connection configuration and connection instance to MSSQL.
    """

    def __init__(self, user, password, host, db_name, driver) -> None:
        # Loading MS SQL Server database credentials
        self.user = user
        self.pw = password
        self.host = host
        self.db = db_name
        self.driver = driver

    def _con_alchemy(self):
        """
        Connection with MSSQL using SQL Alchemy.
        """

        conn = create_engine(
            f"mssql+pyodbc://{self.user}:{self.pw}@{self.host}\
        /{self.db}?driver={self.driver}&NeedODBCTypesOnly=1",
            echo=False,
            pool_pre_ping=False,
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
