from sqlalchemy import (
    DateTime,
    Column,
    Integer,
    func,
    VARCHAR,
    cast,
    FLOAT,
    desc,
    NVARCHAR,
    and_,
)
from sqlalchemy.dialects.mssql import UNIQUEIDENTIFIER
from sqlalchemy.orm import declarative_base
from db.custom_dtypes import Geometry
from db.connection import SQLServerConnection as ssc
from datetime import datetime

Base = declarative_base()


class StravaHour(object):
    # Table columns
    OBJECTID = Column(Integer, primary_key=True, autoincrement=False)
    Shape = Column(Geometry)
    sum_total = Column(Integer)
    hour = Column(NVARCHAR)
    osm_id = Column(Integer)
    date = Column(DateTime)

    __table_args__ = {"schema": "JUDUMAS"}

    def __repr__(self):
        return f"""
            shape: {self.sum_total}, 
        """

    @classmethod
    def load_last_by_hour(cls) -> int:
        session = ssc().create_session()
        data = session.query(func.max((cls.date))).first()[0]

        return data

    @classmethod
    def load_by_date(cls, start_date: datetime, end_date: datetime) -> dict:
        session = ssc().create_session()
        data = (
            session.query(
                cls.date,
                cls.sum_total,
                cls.osm_id,
                cast(cls.Shape, VARCHAR("MAX")).label("Shape"),
            )
            .filter(and_(cls.date >= start_date), cls.date < end_date)
            .all()
        )

        return data


class StravaBikeHour(StravaHour, Base):
    """
    Strava each hour data
    """

    __tablename__ = "STRAVA_BIKE_HOUR"


class StravaPedestrianHour(StravaHour, Base):
    """
    Strava each hour data
    """

    __tablename__ = "STRAVA_PEDESTRIAN_HOUR"
