from sqlalchemy.types import UserDefinedType
from sqlalchemy import text


class Geometry(UserDefinedType):
    """
    MSSQL Geometry datatype
    """

    cache_ok = True

    def get_col_spec(self):
        return "GEOMETRY"

    def bind_expression(self, bindvalue):
        expression = text(
            f"GEOMETRY::STGeomFromText(:{bindvalue.key}, 4326)"
        ).bindparams(bindvalue)

        return expression
