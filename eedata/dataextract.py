import os
import pickle
import platform
import random
import re
import string
import sys
from datetime import datetime, timezone

import pandas as pd
from azure.data.tables import TableServiceClient
from IPython.display import Markdown, display

if sys.platform == "darwin" and platform.processor() == "arm":
    import pymssql
else:
    import pyodbc

def printmd(string):
    display(Markdown(string))


class DataSource:
    """
    A class for managing sources of data for the data extract. The sources are read from SQL or
    Table Storage and then stored in dataframes. From there we clean and pseudonomise the columns.
    Various sources can be combined to create extracts.
    """

    def __init__(self, name, dataframe, path="data", parents=None):
        self.name = name
        self.df = dataframe

        self.is_pseudonomised = False
        self.columns_to_entities = dict()
        self.whitelist = set()

        self.glossary = dict()
        self.path = path

        self.parents = parents

    @classmethod
    def fromMerge(cls, name, parent_data_sources, merge_func):
        dataframe = merge_func(parent_data_sources)

        return cls(name=name, dataframe=dataframe, parents=parent_data_sources)

    @classmethod
    def fromSQL(
        cls, name, sql, connection_string_name="AZURE_SQL_REPORT_CONNECTION_STRING"
    ):
        connection_string = os.getenv(connection_string_name)
        cnxn = pyodbc.connect(connection_string)
        cursor = cnxn.cursor()

        cursor.execute(sql)

        # with open(os.path.join(path, "sql-from-python-experiment.csv"), "w", newline="", encoding="utf-8") as csvfile:
        #     writer = csv.writer(csvfile)
        #     writer.writerow([x[0] for x in cursor.description])
        #     row = cursor.fetchone()
        #     while row:
        #         writer.writerow(row)
        #         row = cursor.fetchone()

        columns = [d[0] for d in cursor.description]
        rows = [list(i) for i in cursor.fetchall()]
        dataframe = pd.DataFrame(rows, columns=columns)

        return cls(name=name, dataframe=dataframe)

    @classmethod
    def fromSQLFile(
        cls, name, filename, connection_string_name="AZURE_SQL_REPORT_CONNECTION_STRING"
    ):
        connection_string = os.getenv(connection_string_name)
        cnxn = pyodbc.connect(connection_string)
        cursor = cnxn.cursor()

        with open(filename, "r") as file:
            sql = file.read()

        cursor.execute(sql)

        columns = [d[0] for d in cursor.description]
        rows = [list(i) for i in cursor.fetchall()]
        dataframe = pd.DataFrame(rows, columns=columns)

        return cls(name=name, dataframe=dataframe)

    @classmethod
    def fromTableStorage(cls, name, table_name, query_filter, connection_string_name):
        connection_string = os.getenv(connection_string_name)
        table_service_client = TableServiceClient.from_connection_string(
            conn_str=connection_string
        )
        table_client = table_service_client.get_table_client(
            table_name=table_name)

        entities = table_client.query_entities(query_filter)
        dataframe = pd.DataFrame(entities)

        return cls(name=name, dataframe=dataframe)

    @classmethod
    def fromCSV(cls, name, csv_name):
        dataframe = pd.read_csv(csv_name)

        return cls(name=name, dataframe=dataframe)

    def export(self):
        """
        Export the data source as a csv. The intention is that this can be shared with others and
        should therefore not contain any sensitive information.
        """
        assert (
            self.is_pseudonomised == True
        ), "You can only export data which has been pseudonomised."

        self.df.to_csv(os.path.join(self.path, self.name, ".csv"), index=False)

    def update_glossary(self, d):
        """
        Update the glossary with additional fields. Existing entries will be overwritten.
        """
        self.glossary = dict(self.glossary, **d)

    def save_readme(self):
        md = f"""\
# Data Source Documentation

{self.name}

## Glossary

| Name | Description |
|------|-------------|
"""
        for k, v in self.glossary.items():
            md += f"| {k} | {v} |\n"

        p = os.path.join(self.path, self.name)

        if not os.path.exists(p):
            os.mkdir(p)

        with open(os.path.join(p, f"readme.md"), "w") as f:
            f.writelines(md)


class DataExtract:
    """
    A class for managing data extracts.
    """

    def __init__(self, name, description, path="data"):
        """
        Initialisation

        Parameters
        ----------
        name : str
            A short human-friendly name for the data extract.
        description : str
            A longer explanation of what this particular extract is, why it was created, who it is
            for, etc.

        """

        self.name = name
        self.description = description
        self.path = path

        self.data_sources = {}
        self.pseudonomisation = {}

        self.__stamp()

    def __str__(self):
        return (
            f"Name:\t\t{self.name}\n"
            + f"Id:\t\t{self.id}\n"
            + f"Datetime:\t{self.datetime}\n"
            + f"Description:\t{self.description}"
        )

    def __stamp(self):
        """
        Stamps the data extract with a unique code and datetime.

        """
        size = 32
        chars = string.ascii_lowercase + string.digits

        self.id = "".join(random.choices(chars, k=size))
        self.datetime = datetime.now(timezone.utc)

    def add_data_source(self, data_source):
        self.data_sources[data_source.name] = data_source

    def get_or_set_data_source(self, data_source_name, data_source_func, **kwargs):
        ds = self.data_sources.get(data_source_name, None)

        if ds is None:
            ds = data_source_func(data_source_name, **kwargs)
            self.add_data_source(ds)
        
        return ds

    def save(self):
        """
        Save the data extract.
        """
        filename = "-".join([self.name, self.id]) + ".pkl"
        with open(os.path.join(self.path, filename), "wb") as file:
            pickle.dump(self, file)

    def export(self, data_source_names=None):
        """
        Export the data extract as a csv. The intention is that this can be shared with others and
        should therefore not contain any sensitive information.

        Parameters
        ----------
        data_source_names : list
            A list of data source names to export. If None, then all data sources will be exported.
        """
        if data_source_names is None:
            data_source_names = self.data_sources.keys()

        for name in data_source_names:
            ds = self.data_sources[name]
            ds.export()

    @staticmethod
    def load(filename, path="data"):
        """
        Load a data extract from the given filename.

        Usage
        -----
        d = DataExtract.load("foo.pkl")
        """
        with open(os.path.join(path, filename), "rb") as file:
            return pickle.load(file)

    def find_id_columns(self):
        return {i for i in self.df.columns if re.search(r"[i|I][d|D]$", i)}

    def pseudonomise(self, data_source_name):
        """
        Changes the values in the specified dataframe columns so they cannot be linked to our
        database.

        Parameters
        ----------
        data_source_name : str
            The name of the data source to process.
        columns_to_entities : dict
            A dictionary of column names to entity names. For example a column
            "CorrectionsQuizSessionId" would be mapped to the entity "QuizSessionId".
        whitelist : list
            A list of all the other columns which may look like ids but do not need to be processed.
        is_pseudonomised : bool
            Whether or not the dataframe has been processed.
        """

        data_source = self.data_sources.get(data_source_name, None)

        assert data_source is not None, "The data source name is not found."

        assert (
            data_source.is_pseudonomised == False
        ), "This data frame has already been pseudonomised."

        dataframe = data_source.df
        columns_to_entities = data_source.columns_to_entities
        whitelist = data_source.whitelist

        id_columns = {
            i for i in dataframe.columns if re.search(r"[i|I][d|D]$", i)}

        mapped_columns = set(columns_to_entities.keys())

        missed_columns = id_columns - mapped_columns - whitelist

        assert (
            missed_columns == set()
        ), f"The columns {'|'.join(missed_columns)} should either be mapped or in the whitelist."

        for col, ent in columns_to_entities.items():
            existing_real_id_to_pseudo_id = self.pseudonomisation.get(ent, {})

            new_real_id_to_pseudo_id = self.__real_id_to_pseudo_id(
                dataframe[col], existing_real_id_to_pseudo_id
            )

            self.pseudonomisation[ent] = new_real_id_to_pseudo_id

            dataframe[col] = dataframe[col].map(new_real_id_to_pseudo_id)

        data_source.is_pseudonomised = True

    def __real_id_to_pseudo_id(self, real_ids, real_id_to_pseudo_id={}):
        """
        Returns two dictionaries for the given real ids. One from the real ids to
        some pseudo ids and the other simply the reverse dictionary.

        Parameters
        ----------
        real_ids : series
            A series of integer values we want to map to pseudo ids.

        real_id_to_pseudo_id : dict (opt)
            A dictionary mapping real_ids to pseudo_ids. If present then update.

        Returns
        -------
        dict
            A dictionary mapping real to pseudo ids.

        """

        real_ids = real_ids.dropna()
        real_ids = real_ids.drop_duplicates()
        real_ids = real_ids.astype(int)

        existing_real_ids = real_id_to_pseudo_id.keys()

        new_real_ids = real_ids[~real_ids.isin(existing_real_ids)]

        start_pseudo_id = (
            max(real_id_to_pseudo_id.values()) +
            1 if real_id_to_pseudo_id != {} else 0
        )

        new_real_id_to_pseudo_id = {
            new_real_id: i + start_pseudo_id
            for i, new_real_id in enumerate(new_real_ids)
        }

        real_id_to_pseudo_id = {
            **real_id_to_pseudo_id, **new_real_id_to_pseudo_id}

        assert len(real_id_to_pseudo_id.keys()) == len(
            set(real_id_to_pseudo_id.values())
        ), "At least two keys are mapped to the same value."

        pseudo_id_to_real_id = {y: x for x, y in real_id_to_pseudo_id.items()}

        return real_id_to_pseudo_id

    @staticmethod
    def get_data_source_from_list(name, data_sources):
        try:
            return next(x for x in data_sources if x.name == name)
        except:
            print(f"There is no data source with the name {name}.")
