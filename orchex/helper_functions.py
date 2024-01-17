"""Helper functions for Orchex."""

import datetime
import os
import platform
import sys
import time
from collections.abc import Iterable

import pandas as pd
import pyodbc


def create_join_identifiers_table(
    temp_table_name: str, identifier_name: str, identifier_set: Iterable
):
    """Creates a temp table with a single column of identifiers to be used in a join.

    Args:
        temp_table_name (str): Name of the temp table to be created.
        identifier_name (str): Name of the column in the temp table.
        identifier_set (Iterable): List of identifiers to be inserted into the temp table.
    """
    # check temp table starts with #
    assert (
        temp_table_name[0] == "#"
    ), "This function is designed to create a temp table, please provide a name starting with #."

    middle_bit = f") INSERT INTO {temp_table_name} VALUES ("

    return f"""DROP TABLE IF EXISTS {temp_table_name}
CREATE TABLE {temp_table_name} ({identifier_name} INT)
INSERT INTO {temp_table_name} VALUES {'(' + middle_bit.join(list(map(str, identifier_set))) + ')'}"""


def _SQLconnection(connection_string_name="AZURE_SQL_REPORT_CONNECTION_STRING"):
    if sys.platform == "darwin" and platform.processor() == "arm":
        server = os.getenv("server")
        user = os.getenv("user_id")
        password = os.getenv("password")
        database = "diagnosticquestions-report"
        cnxn = pymssql.connect(server, user, password, database)
    else:
        connection_string = os.getenv(connection_string_name)
        cnxn = pyodbc.connect(connection_string)
    cursor = cnxn.cursor()
    return cursor


def update_sql(
    sql: str, connection_string_name: str = "AZURE_SQL_REPORT_CONNECTION_STRING"
):
    """Executes an update SQL query.

    Args:
        sql (str): SQL query to be executed.
        connection_string_name (str, optional): Name of the environment variable containing the connection string. Defaults to "AZURE_SQL_REPORT_CONNECTION_STRING".
    """
    cursor = _SQLconnection(connection_string_name)

    try:
        cursor.execute(sql)
    except Exception as e:
        print("Error executing SQL query:", e)
        return None

    cursor.commit()
    return None


def getDateTable(start: str, end: str = None):
    """Creates a date dimension table.

    Args:
        start (str): Start date of the date dimension table.
        end (str, optional): End date of the date dimension table. Defaults to None.
    """
    if not end:
        end = datetime.datetime.today().strftime("%Y-%m-%d")

    inital_dates = pd.DataFrame(
        data=pd.date_range(start=start, end=end), columns=["Date"]
    )

    date_dimension = inital_dates.assign(
        UnixTimestamp=inital_dates["Date"].apply(
            lambda x: int(time.mktime(x.timetuple()))
        ),
        DateKey=inital_dates["Date"].dt.strftime("%Y%m%d").astype(int),
        DateDayFirst=inital_dates["Date"].dt.strftime("%d-%m-%Y"),
        DateUSA=inital_dates["Date"].dt.strftime("%Y-%d-%m"),
        Year=inital_dates["Date"].dt.year,
        Month=inital_dates["Date"].dt.month,
        Day=inital_dates["Date"].dt.day,
        DayOfYear=inital_dates["Date"].dt.dayofyear,
        DayName=inital_dates["Date"].dt.day_name(),
        IsWeekend=lambda x: x["DayName"].isin(["Saturday", "Sunday"]),
        DayOfWeekMon0=inital_dates["Date"].dt.dayofweek,
        DayOfWeekSun0=(inital_dates["Date"].dt.dayofweek + 1) % 7,
        ISOWeekOfYear=inital_dates["Date"].dt.isocalendar().week,
        MonthName=inital_dates["Date"].dt.month_name(),
        MonthNameAbbr=lambda x: x["MonthName"].str[0:3],
        Quarter=inital_dates["Date"].dt.quarter,
    )
    return date_dimension
