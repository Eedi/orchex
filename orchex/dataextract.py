import os
import pickle
import platform
import random
import re
import string
import sys
from datetime import datetime, timezone
from typing import Union
from pathlib import Path
import inflect

import pandas as pd
from azure.data.tables import TableServiceClient

p = inflect.engine()

if sys.platform == "darwin" and platform.processor() == "arm":
    import pymssql
else:
    import pyodbc

class MarkdownReport:

    def __init__(self, title):
        self.report = [
            f"# {title}"
        ]

    def add_heading(self, heading, level=1, anchor=None):
        heading = f"{'#' * level} {heading}"
        if anchor:
            heading = heading + f" <a name='{anchor}' id='{anchor}'></a>"
        self.report.append(heading)

    def add_text(self, text):
        self.report.append(text)

    def add_list(self, items, ordered=False):
        list_prefix = "1. " if ordered else "- "
        for item in items:
            self.report.append(f"{list_prefix}{item}")

    def add_image(self, image_path, alt_text=""):
        self.report.append(f"![{alt_text}]({image_path})")

    def add_definitions(self, definitions: Union[list[list], dict]):
        if isinstance(definitions, dict):
            definitions = list(definitions.items())

        formatted_definitions = []
        for term, definition in definitions:
            # Preserve intentional line breaks
            fd = str(definition).replace("\n\n", "<br><br>")

            # Remove line breaks that are not intentional. The regex is
            # required to prevent \\neq being matched (it appears in LaTeX).
            fd = re.sub(r"(?<!\\)\n", " ", fd)

            formatted_definitions.append((term, fd))

        headers = ["Term", "Definition"]
        self.add_table(headers, formatted_definitions)

    def add_table(self, headers, rows):
        table = [f"| {' | '.join(headers)} |", f"| {' | '.join(['---'] * len(headers))} |"]
        for row in rows:
            table.append(f"| {' | '.join(str(item) for item in row)} |")
        self.report.append("\n".join(table))

    def add_code_block(self, code, language=""):
        self.report.append(f"```{language}\n{code}\n```")

    def add_blockquote(self, quote):
        self.report.append(f"> {quote}")

    def add_horizontal_rule(self):
        self.report.append("---")

    def add_dataframe(self, df):
        md = df.to_markdown()

        self.report.append(f"""<div style="overflow-x: auto;">\n\n{md}\n\n</div>""")

    def add_markdown(self, markdown):
        self.report.append(markdown)

    def add_table_of_contents(self, headings_dict):
        toc_markdown, toc_anchors = self._generate_table_of_contents(headings_dict)

        self.report.append(toc_markdown)

        return toc_anchors

    def _generate_table_of_contents(self, headings_dict, toc_markdown="", toc_anchors=dict(), level=0):
        """A (too) simple toc generator which can fail if any headings have the same name.

        Args:
            headings_dict (_type_): A dictionary of headings where the key is the heading name and the
                value is a nested dictionary of subheadings. E.g.
                headings_dict = {
                    "Introduction": None,
                    "Data Sources": {
                        "data_source_1": None,
                        "data_source_2": None
                    }
                }
            toc_markdown (str, optional): A concatenated markdown string, which should not be set
                manually. It is in the function parameters so it can be called recursively. Defaults to
                "".
            toc_anchors (_type_, optional): A dictionary of anchors which will be used to define the
                anchors in the markdown document, e.g. <a name="foo"></a>. It should not be set
                manually. Defaults to dict().
            level (int, optional): The level of indent for the toc. Defaults to 0.

        Returns:
            tuple: The markdown string for the toc and the dictionary of anchors.
        """

        index = 1

        for heading in headings_dict:
            anchor = heading.lower().replace("/","_").replace(" ","-")

            toc_markdown += f"{'    ' * level}{index}. [{heading}](#{anchor})\n"

            # Generate a unique anchor for the heading
            assert toc_anchors.get(heading) == None, \
                "This heading already exists, use unique names for headings (or write a better toc \
                generator)."

            toc_anchors[heading] = anchor

            # Recursively cycle through subheadings
            subheadings_dict = headings_dict[heading]
            if subheadings_dict:
                toc_markdown, toc_anchors = self._generate_table_of_contents(subheadings_dict, toc_markdown, toc_anchors, level=level+1)

            index += 1

        return toc_markdown, toc_anchors

    def save(self, filepath):
        with open(filepath, "w", encoding="utf-8") as f:
            f.write("\n\n".join(self.report))

class DataSource:
    """
    A class for managing sources of data for the data extract. The sources are read from SQL or
    Table Storage and then stored in dataframes. From there we clean and pseudonomise the columns.
    Various sources can be combined to create extracts.
    """

    def __init__(self, name, dataframe, parents=None, glossary=dict(),
                 columns_to_entities=dict(), whitelist=set(), data_extract=None,
                 description=None):
        
        self.name = name
        self.description = description
        self.df = dataframe
        self.columns_to_entities = columns_to_entities
        self.whitelist = whitelist

        self.is_pseudonomised = False

        self.glossary = glossary

        self.parents = parents

        self.data_extract = data_extract

    @classmethod
    def fromMerge(cls, name, parent_data_sources, merge_func, **kwargs):
        dataframe = merge_func(parent_data_sources)

        return cls(name=name, dataframe=dataframe, parents=parent_data_sources, **kwargs)

    @classmethod
    def fromSQL(
        cls, name, sql, connection_string_name="AZURE_SQL_REPORT_CONNECTION_STRING", **kwargs
    ):
        connection_string = os.getenv(connection_string_name)
        cnxn = pyodbc.connect(connection_string)
        cursor = cnxn.cursor()

        cursor.execute(sql)

        # with open("sql-from-python-experiment.csv", "w", newline="", encoding="utf-8") as csvfile:
        #     writer = csv.writer(csvfile)
        #     writer.writerow([x[0] for x in cursor.description])
        #     row = cursor.fetchone()
        #     while row:
        #         writer.writerow(row)
        #         row = cursor.fetchone()

        columns = [d[0] for d in cursor.description]
        rows = [list(i) for i in cursor.fetchall()]
        dataframe = pd.DataFrame(rows, columns=columns)

        return cls(name=name, dataframe=dataframe, **kwargs)

    @classmethod
    def fromSQLFile(
        cls, name, filename, connection_string_name="AZURE_SQL_REPORT_CONNECTION_STRING", **kwargs
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

        return cls(name=name, dataframe=dataframe, **kwargs)

    @classmethod
    def fromTableStorage(cls, name, table_name, query_filter, connection_string_name, **kwargs):
        connection_string = os.getenv(connection_string_name)
        table_service_client = TableServiceClient.from_connection_string(
            conn_str=connection_string
        )
        table_client = table_service_client.get_table_client(
            table_name=table_name)

        entities = table_client.query_entities(query_filter)
        dataframe = pd.DataFrame(entities)

        return cls(name=name, dataframe=dataframe, **kwargs)

    @classmethod
    def fromCSV(cls, name, csv_name, **kwargs):
        dataframe = pd.read_csv(csv_name)

        return cls(name=name, dataframe=dataframe, **kwargs)

    def set_data_extract(self, data_extract):
        self.data_extract = data_extract

    def export(self, export_path: Union[str, Path] = None):
        """
        Export the data source as a csv. The intention is that this can be shared with others and
        should therefore not contain any sensitive information.

        Parameters
        ----------
        export_path : str|Path
            The path to the folder where the csv should be saved. Typically this will come from the
            data extract and be the following format:

            (self.data_extract_path / self.public_folder / "data")

            A csv with the name of the data source will be saved to this folder.
        """
        assert (
            self.is_pseudonomised == True
        ), "You can only export data which has been pseudonomised."

        if export_path is None and self.data_extract:
            export_path = (self.data_extract.data_extract_path /
                self.data_extract.public_folder / "data")

        if isinstance(export_path, str):
            export_path = Path(export_path)

        self.df.to_csv(str(export_path /  f"{self.name}.csv"), index=False)

    def update_glossary(self, d):
        """
        Update the glossary with additional fields. Existing entries will be overwritten.
        """
        self.glossary = dict(self.glossary, **d)
    

    def _get_summary_statistics_field(self, field):
        
        def _get_common_stats(field):
            return {
                "field": field.name,
                "count": field.count(),
                "nunique": field.nunique(),
                "non-null": field.notnull().sum()
            }

        def _handle_int_stats(field):
            stats = _get_common_stats(field)
            stats.update({
                "type": "int",
                "min": field.min(),
                "max": field.max(),
            })

            if stats["nunique"] < 10:
                stats["value_counts"] = str(field.value_counts().to_dict())

            return stats

        def _handle_float_stats(field):
            # Check whether all non-null values are integers
            if field.dropna().apply(float.is_integer).all():
                return _handle_int_stats(field)

            stats = get_common_stats(field)
            stats.update({
                "type": "float",
                "min": field.min(),
                "25%": field.quantile(0.25),
                "50%": field.quantile(0.50),
                "75%": field.quantile(0.75),
                "max": field.max()
            })
            
            return stats

        def _handle_bool_stats(field):
            stats = _get_common_stats(field)
            stats.update({
                "type": "bool",
                "value_counts": str(field.value_counts().to_dict())
            })
            
            return stats

        def _handle_datetime_stats(field):
            stats = _get_common_stats(field)
            stats.update({
                "type": "datetime",
                "min": field.min(),
                "max": field.max()
            })

            return stats

        def _handle_str_stats(field):
            stats = _get_common_stats(field)
            stats.update({
                "type": "object",
                "min_length": field.str.len().min(),
                "max_length": field.str.len().max(),
                "min_words": field.str.count(" ").min() + 1,
                "max_words": field.str.count(" ").max() + 1
            })

            if stats["nunique"] < 10:
                stats["value_counts"] = str(field.value_counts().to_dict())

            return stats

        def _handle_default(field):
            stats = _get_common_stats(field)
            stats.update({
                "type": "unknown"
            })

            return stats
        
        dtype = field.dtype

        if pd.api.types.is_integer_dtype(dtype):
            return _handle_int_stats(field)
        elif pd.api.types.is_float_dtype(dtype):
            return _handle_float_stats(field)
        elif pd.api.types.is_string_dtype(dtype):
            return _handle_str_stats(field)
        elif pd.api.types.is_bool_dtype(dtype):
            return _handle_bool_stats(field)
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return _handle_datetime_stats(field)
        else:
            return _handle_default(field)

    def get_summary_statistics(self):
        stats = []
        for field in self.df:
            stats.append(self._get_summary_statistics_field(self.df[field]))

        return pd.DataFrame(stats)

    def add_markdown_report(self, markdown_report, anchor):

        plural_entity_name = p.plural(self.name)

        df = self.df

        markdown_report.add_heading(self.name, 3, anchor)

        if self.description:
            markdown_report.add_markdown(self.description)

        markdown_report.add_heading("Glossary", 4)

        if self.glossary:
            formatted_glossary = {k: v.replace("\n\n", "<br><br>") for k, v in self.glossary.items()}
            markdown_report.add_definitions(formatted_glossary)

        markdown_report.add_heading("Statistics", 4)

        summary_statistics = self.get_summary_statistics()

        markdown_report.add_dataframe(summary_statistics)

        markdown_report.add_heading("Sample", 4)

        if (self.df.shape[0] > 0):
            markdown_report.add_dataframe(self.df.sample(3))

class DataExtract:
    """
    A class for managing data extracts.
    """

    def __init__(self, name, description, container_path: Union[str, Path] = "data"):
        """
        Initialisation

        Parameters
        ----------
        name : str
            A short human-friendly name for the data extract.
        description : str
            A longer explanation of what this particular extract is, why it was created, who it is
            for, etc.
        container_path : str|Path
            The path to the folder which contains all data extracts. 

        """

        self.name = name
        self.description = description
        if isinstance(container_path, str):
            container_path = Path(container_path)
        self.container_path = container_path

        self.data_sources = {}
        self.pseudonomisation = {}

        self.__stamp()

        self.set_paths_and_filenames()


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

    def set_paths_and_filenames(self):
        """
        Sets the path and filenames for the data extract.
        
        Data is stored in the following format:
        container_path/
            - {name}-{YYYYmmDDHHMM}-{id}
                - {name}-{YYYYmmDDHHMM}-{id}-PRIVATE.pkl
                - {name}-{YYYYmmDDHHMM}-{id}-PUBLIC/
                    - data
                        - {data_source_name}.csv
                    - img
                    - docs
                        - README.md
                        - img

        We define the path to the folder containing the data extract and then the filenames and
        folders within it:

        {data_extract_path} /
            - {private_filename}
            - {public_folder} /
                - data
                    - {data_source_name}.csv
                - img
                - docs
                    - README.md
                    - img
        """
        self.name_date_id = f"{self.name}-{self.datetime.strftime('%Y%m%d%H%M')}-{self.id}"
        self.data_extract_path = self.container_path / self.name_date_id
        self.public_folder = f"{self.name_date_id}-PUBLIC"
        self.public_archive_filename = f"{self.name_date_id}-PUBLIC.zip"
        self.private_filename = f"{self.name_date_id}-PRIVATE.pkl"

        (self.data_extract_path / self.public_folder / "data").mkdir(parents=True, exist_ok=True)
        (self.data_extract_path / self.public_folder / "img").mkdir(parents=True, exist_ok=True)
        (self.data_extract_path / self.public_folder / "docs" / "img").mkdir(parents=True, exist_ok=True)
        
    def add_data_source(self, data_source):
        self.data_sources[data_source.name] = data_source
        data_source.set_data_extract(self)

    def get_or_set_data_source(self, data_source_name, data_source_func, **kwargs):
        ds = self.data_sources.get(data_source_name, None)

        if ds is None:
            ds = data_source_func(data_source_name, **kwargs)
            self.add_data_source(ds)

        return ds

    def get_or_set_data_source_class(self, DataSourceClass, **kwargs):
        ds = self.data_sources.get(DataSourceClass._name, None)

        if ds is None:
            ds = DataSourceClass(**kwargs)
            self.add_data_source(ds)

        return ds

    def save(self):
        """
        Save the data extract.
        """
        filepath = str(self.data_extract_path / self.private_filename)
        
        with open(filepath, "wb") as file:
            pickle.dump(self, file)

    import zipfile
    from pathlib import Path
    from typing import Union

    def zip_folder(folder_to_archive_path: Union[Path, str], archive_file_path):
        folder_to_archive_path = Path(folder_to_archive_path)
        archive_file_path = Path(archive_file_path)

        with zipfile.ZipFile(archive_file_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            for fp in folder_to_archive_path.glob("**/*"):
                zipf.write(fp, arcname=fp.relative_to(folder_to_archive_path))

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

    def archive(self, container_name):
        """
        Archive and upload the data extract to Azure Blob Storage.
        """
        folder_to_archive_path = self.data_extract.data_extract_path / self.data_extract.public_folder
        archive_file_path = self.data_extract.data_extract_path / self.data_extract.public_archive_filename

        zip_folder(folder_to_archive_path, archive_file_path)

        blob_name = os.path.basename(os.getcwd()).replace("_", "-")
        blob = Blobs(container_name)
        blob.upload_file(archive_file_path)

        


    @staticmethod
    def load(filepath: Union[str, Path]):
        """
        Load a data extract from the given filepath.

        Usage
        -----
        d = DataExtract.load(".foo/bar.pkl")  # Using a string
        or
        d = DataExtract.load(Path(".foo/bar.pkl"))  # Using a Path object
        """
        if isinstance(filepath, Path):
            filepath = str(filepath)

        return pd.read_pickle(filepath)

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

    def generate_markdown_report(self):
        """Generate a markdown report for the data extract, reporting statistics on each data
        source.
        """
        markdown_report = MarkdownReport(f"Data analysis for {self.name}")

        # Add a table of contents
        table_of_contents = {
            "Introduction": None,
            "Data Sources": { k: None for k in self.data_sources.keys() }
        }

        anchors = markdown_report.add_table_of_contents(table_of_contents)

        # Add some high level data extract details.
        markdown_report.add_definitions([
            ["Name", self.name],
            ["Id", self.id],
            ["Datetime", self.datetime]
        ])

        # Add an introduction
        markdown_report.add_heading("Introduction", level=1, anchor=anchors["Introduction"])
        markdown_report.add_markdown(self.description)
        
        # Add the data sources
        markdown_report.add_heading("Data Sources", level=1, anchor=anchors["Data Sources"])
        for k, v in self.data_sources.items():
            v.add_markdown_report(markdown_report, anchors[k])

        # Save the report
        markdown_report.save(self.data_extract_path / self.public_folder / "docs" / "README.md")

    @staticmethod
    def get_data_source_from_list(name, data_sources):
        try:
            return next(x for x in data_sources if x.name == name)
        except:
            print(f"There is no data source with the name {name}.")
    