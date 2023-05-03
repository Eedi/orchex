import datetime
import os
import platform
import sys
import time

import gspread
import pandas as pd
from azure.data.tables import TableServiceClient

if sys.platform == "darwin" and platform.processor() == "arm":
    import pymssql
else:
    import pyodbc


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


def fromSQL(sql):
    cursor = _SQLconnection()
    cursor.execute(sql)
    columns = [d[0] for d in cursor.description]
    rows = [list(i) for i in cursor.fetchall()]
    return pd.DataFrame(rows, columns=columns)


def fromTableStorage(
    table_name,
    query_filter,
    connection_string_name="AZURE_STORAGE_EEDIPRODFLOWS_TABLES_CONNECTION_STRING",
):
    connection_string = os.getenv(connection_string_name)
    table_service_client = TableServiceClient.from_connection_string(
        conn_str=connection_string
    )
    table_client = table_service_client.get_table_client(table_name=table_name)

    entities = table_client.query_entities(query_filter)
    return pd.DataFrame(entities)


def fromGSheet(file_key_env_variable, sheet_name=None):
    gc = gspread.service_account(".sheets")
    file_key = os.getenv(file_key_env_variable)
    spreadsheet = gc.open_by_key(file_key)

    if sheet_name:
        worksheet = spreadsheet.worksheet(sheet_name)

    else:
        sheets = spreadsheet.fetch_sheet_metadata()["sheets"]
        sheet_names = [sheets[i]["properties"]["title"] for i in range(len(sheets))]

        if len(sheet_names) > 1:
            return print(
                "WARNING: multiple sheets exist.\n"
                + "Use the keywork 'sheet_name' to specify one of:\n\n",
                "\n".join(sheet_names),
            )

        else:
            worksheet = spreadsheet.get_worksheet(0)
    expected_headers = worksheet.row_values(1)
    return pd.DataFrame(worksheet.get_all_records(expected_headers=expected_headers))


def getDateTable(start: str, end: str = None):
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
