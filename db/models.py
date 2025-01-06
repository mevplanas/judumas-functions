from sqlalchemy import (
    DateTime,
    Column,
    Integer,
    func,
    VARCHAR,
    or_
)
from sqlalchemy.orm import declarative_base

from db.connection import SQLServerConnection as ssc


Base = declarative_base()

class OpRoutesDates(Base):
    """
    Description:
    ------------
    OpRoutesDates table model class.
    """

    __tablename__ = "OP_ROUTES_DATES"

    OBJECTID = Column(Integer, primary_key=True, autoincrement=False)
    date_str = Column(VARCHAR)
    dates_bikes = Column(VARCHAR)
    dates_vt = Column(VARCHAR)
    dates_pedestrians = Column(VARCHAR)
    updated_at = Column(DateTime, default=func.now())
    created_at = Column(DateTime, default=func.now())

    __table_args__ = {"schema": "JUDUMAS"}

    @classmethod
    def get_max_id(cls, conn: ssc) -> int:
        """
        Description:
        ------------
        Get the maximum OBJECTID value from the table.

        Returns:
        ------------
        :return: Maximum OBJECTID value.
        """
        session = conn.create_session()
        try:
            max_id = session.query(func.max(cls.OBJECTID)).scalar()
            return max_id
        except Exception as e:
            raise e
        finally:
            session.close()

    @classmethod
    def check_and_add_pedestrians_date(
        cls, conn: ssc, date_string: str, object_id: int
    ) -> None:
        """
        Description:
        ------------
        Check if the given date exists in the `date_str` and `dates_pedestrians` columns.
        - If the date doesn't exist in either, add it to both.
        - If it exists in `date_str` but not in `dates_pedestrians`, update `dates_pedestrians`.

        Parameters:
        ------------
        :param conn: Database connection object.
        :param date_string: Date value to check and potentially add.
        :param object_id: OBJECTID value to be inserted with the date if needed.
        """

        session = conn.create_session()
        try:
            # Query the row with the matching date_str
            existing_row = (
                session.query(cls).filter(or_(cls.date_str == date_string, cls.dates_pedestrians == date_string, cls.dates_vt == date_string, cls.dates_bikes == date_string)).first()
            )

            if not existing_row:
                # If the date doesn't exist in either column, insert a new row with the date in both columns
                new_row = cls(
                    OBJECTID=object_id,
                    dates_pedestrians=date_string,
                )
                session.add(new_row)
            else:
                # If the date exists in `date_str` but not in `dates_pedestrians`, update the row
                if not existing_row.dates_pedestrians:
                    existing_row.dates_pedestrians = date_string

            # Commit the changes
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    @classmethod
    def check_and_add_public_transport_date(cls, conn: ssc, date_string: str, object_id: int) -> None:
        """
        Description:
        ------------
        Check if the given date exists in the `date_str` and `dates_vt` columns.
        - If the date doesn't exist in either, add it to both.
        - If it exists in `date_str` but not in `dates_vt`, update `dates_vt`.

        Parameters:
        ------------
        :param conn: Database connection object.
        :param date_string: Date value to check and potentially add.
        :param object_id: OBJECTID value to be inserted with the date if needed.
        """

        session = conn.create_session()
        try:
            # Query the row with the matching date_str
            existing_row = (
                session.query(cls).filter(or_(cls.date_str == date_string, cls.dates_pedestrians == date_string, cls.dates_vt == date_string, cls.dates_bikes == date_string)).first()
            )

            if not existing_row:
                # If the date doesn't exist in either column, insert a new row with the date in both columns
                new_row = cls(
                    OBJECTID=object_id,
                    dates_vt=date_string,
                )
                session.add(new_row)
            else:
                # If the date exists in `date_str` but not in `dates_vt`, update the row
                if not existing_row.dates_vt:
                    existing_row.dates_vt = date_string

            # Commit the changes
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()


    @classmethod
    def check_and_add_bikes(cls, conn: ssc, date_string: str, object_id: int) -> None:
        """
        Description:
        ------------
        Check if the given date exists in the `date_str` and `dates_bikes` columns.
        - If the date doesn't exist in either, add it to both.
        - If it exists in `date_str` but not in `dates_bikes`, update `dates_bikes`.

        Parameters:
        ------------
        :param conn: Database connection object.
        :param date_string: Date value to check and potentially add.
        :param object_id: OBJECTID value to be inserted with the date if needed.
        """

        session = conn.create_session()
        try:
            # Query the row with the matching date_str
            existing_row = (
                session.query(cls).filter(or_(cls.date_str == date_string, cls.dates_pedestrians == date_string, cls.dates_vt == date_string, cls.dates_bikes == date_string)).first()
            )

            if not existing_row:
                # If the date doesn't exist in either column, insert a new row with the date in both columns
                new_row = cls(
                    OBJECTID=object_id,
                    dates_bikes=date_string,
                )
                session.add(new_row)
            else:
                # If the date exists in `date_str` but not in `dates_bikes`, update the row
                if not existing_row.dates_bikes:
                    existing_row.dates_bikes = date_string

            # Commit the changes
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()