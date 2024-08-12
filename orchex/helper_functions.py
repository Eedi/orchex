"""Helper functions for Orchex."""
import datetime
import os
import time
from collections.abc import Iterable
from textwrap import dedent

import pandas as pd
import pyodbc
from mnemonic import Mnemonic


def create_join_identifiers_table(
    temp_table_name: str,
    id_name: str,
    id_set: Iterable,
    is_int: bool = True,
    batch_size: int = 1000,
):
    """Creates a temp table with a single column of identifiers to be used in a join.

    Args:
        temp_table_name (str): Name of the temp table to be created.
        identifier_name (str): Name of the column in the temp table.
        identifier_set (Iterable): List of identifiers to be inserted into the temp table.
        is_int (bool, optional): Whether the identifiers are integers. Defaults to True.
        batch_size (int, optional): Batch size for each 'INSERT INTO' command. Defaults to our database limit 1000.
    """
    # Check temporary table starts with '#'
    assert (
        temp_table_name[0] == "#"
    ), "This function is designed to create a temp table, please provide a name starting with #."

    # Create the initial table creation string
    dtype = "INT" if is_int else "VARCHAR(255)"

    initial_string = f"""\
        DROP TABLE IF EXISTS {temp_table_name}
        CREATE TABLE {temp_table_name} ({id_name} {dtype})
        """

    # Create the batched insertion string
    id_list = list(id_set)
    n_batches = (len(id_list) - 1) // batch_size

    def _generate_batch_string(batch: list) -> str:
        if is_int:
            batch_string = ", ".join(f"({value})" for value in batch)
        else:
            # Need single '' around each value when using strings
            batch_string = ", ".join(f"('{value}')" for value in batch)

        return batch_string

    insert_string = ""
    for i in range(0, n_batches + 1):
        batch = id_list[i * batch_size : (i + 1) * batch_size]
        insert_string += (
            f"INSERT INTO {temp_table_name} VALUES {_generate_batch_string(batch)}\n"
        )

    return dedent(initial_string) + dedent(insert_string)


def _SQLconnection(
    connection_string_name="AZURE_SQL_REPORT_CONNECTION_STRING",
    encoding: str | None = None,
):
    connection_string = os.getenv(connection_string_name)

    # drivers argument added to work with drivers from all platforms (Windows, Linux, Mac)
    cnxn = pyodbc.connect(connection_string, driver=str(pyodbc.drivers()[0]))

    if encoding:
        cnxn.setdecoding(pyodbc.SQL_CHAR, encoding=encoding)
        cnxn.setdecoding(pyodbc.SQL_WCHAR, encoding=encoding)

    cursor = cnxn.cursor()
    return cursor


def update_sql(
    sql: str,
    connection_string_name: str = "AZURE_SQL_REPORT_CONNECTION_STRING",
    encoding: str | None = None,
):
    """Executes an update SQL query.

    Args:
        sql (str): SQL query to be executed.
        connection_string_name (str, optional): Name of the environment variable containing the connection string. Defaults to "AZURE_SQL_REPORT_CONNECTION_STRING".
        encoding (str, optional): Encoding to be used. Defaults to None.
    """
    cursor = _SQLconnection(connection_string_name, encoding)

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


def three_word_identifier() -> str:
    """Generates an _almost certainly_ unique three word identifier.

    Only to be used in contexts where _almost certainly unique_ is OK.

    Returns:
        str: An _almost certainly_ unique three word identifier of the form "foo-bar-boo".
    """
    mnemo = Mnemonic("english")
    words = mnemo.generate(strength=128).split()

    return "-".join(words[:3])
