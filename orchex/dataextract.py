"""A module for managing data extracts, including the DataExtract and DataSource classes."""

import os
import pathlib
import pickle
import platform
import random
import re
import string
import zipfile
from collections.abc import Callable
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import inflect
import pandas as pd
from azure.data.tables import TableServiceClient

from .blobs import Blobs
from .helper_functions import _SQLconnection


@contextmanager
def set_posix_windows():
    """A context manager for temporarily changing the behaviour of Pathlib's PosixPath and WindowsPath."""
    plt = platform.system()
    if plt == "Windows":
        posix_backup = pathlib.PosixPath
        try:
            pathlib.PosixPath = pathlib.WindowsPath
            yield
        finally:
            pathlib.PosixPath = posix_backup
    else:
        posix_backup = pathlib.WindowsPath
        try:
            pathlib.WindowsPath = pathlib.PosixPath
            yield
        finally:
            pathlib.WindowsPath = posix_backup  # noqa: F821


p = inflect.engine()


def zip_folder(folder_to_archive_path: Path | str, archive_file_path: Path | str):
    """Zip the contents of a given folder and save to a given file path.

    Args:
        folder_to_archive_path (Path | str): The folder to archive.
        archive_file_path (Path | str): The path to the archive file.
    """
    folder_to_archive_path = Path(folder_to_archive_path)
    archive_file_path = Path(archive_file_path)

    with zipfile.ZipFile(archive_file_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for fp in folder_to_archive_path.glob("**/*"):
            zipf.write(fp, arcname=fp.relative_to(folder_to_archive_path))


class MarkdownReport:
    """A class for generating markdown reports."""

    def __init__(self, title: str):
        """Initialise the markdown report with the given title.

        Args:
            title (str): The title to put at the top of the report.
        """
        self.report = [f"# {title}"]

    def add_heading(self, heading: str, level: int = 1, anchor: str = None):
        """Add a heading to the markdown report.

        Args:
            heading (str): The text of the heading.
            level (int, optional): The level of the heading (default is 1).
            anchor (str, optional): The anchor name for the heading (default is None).
        """
        heading = f"{'#' * level} {heading}"
        if anchor:
            heading = heading + f" <a name='{anchor}' id='{anchor}'></a>"
        self.report.append(heading)

    def add_text(self, text: str):
        """Add text to the markdown report.

        Args:
            text (str): The text to add to the markdown report.
        """
        self.report.append(text)

    def add_list(self, items: list[str], ordered: bool = False):
        """Add a list of items to the markdown report.

        Args:
            items (list[str]): The list of items to add.
            ordered (bool, optional): Whether the list should be ordered (default is False).
        """
        list_prefix = "1. " if ordered else "- "
        for item in items:
            self.report.append(f"{list_prefix}{item}")

    def add_image(self, image_path: str, alt_text: str = ""):
        """Add an image to the markdown report.

        Args:
            image_path (str): The path to the image.
            alt_text (str, optional): The alt text for the image. Defaults to "".
        """
        self.report.append(f"![{alt_text}]({image_path})")

    def add_definitions(self, definitions: list[list] | dict):
        """Add a list of definitions to the markdown report.

        Args:
            definitions (list[list] | dict): A list of definitions, which can be a list of lists or a dictionary.
        """
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

    def add_table(self, headers: list[str], rows: list[str]):
        """Add a table to the markdown report.

        Args:
            headers (list[str]): The table headers.
            rows (list[str]): The table rows.
        """
        table = [
            f"| {' | '.join(headers)} |",
            f"| {' | '.join(['---'] * len(headers))} |",
        ]
        for row in rows:
            table.append(f"| {' | '.join(str(item) for item in row)} |")
        self.report.append("\n".join(table))

    def add_code_block(self, code: str, language: str = ""):
        """Add a code block to the markdown report.

        Args:
            code (str): The code to add to the markdown report.
            language (str, optional): The language of the code. Defaults to "".
        """
        self.report.append(f"```{language}\n{code}\n```")

    def add_blockquote(self, quote: str):
        """Add a blockquote to the markdown report.

        Args:
            quote (str): The quote to add to the markdown report.
        """
        self.report.append(f"> {quote}")

    def add_horizontal_rule(self):
        """Add a horizontal rule to the markdown report."""
        self.report.append("---")

    def add_dataframe(self, df: pd.DataFrame):
        """Add a dataframe to the markdown report.

        Args:
            df (pd.DataFrame): The dataframe to add to the markdown report.
        """
        md = df.to_markdown()

        self.report.append(f"""<div style="overflow-x: auto;">\n\n{md}\n\n</div>""")

    def add_markdown(self, markdown):
        """Add markdown to the markdown report.

        Args:
        markdown (str): The markdown to add to the markdown report.
        """
        self.report.append(markdown)

    def add_table_of_contents(self, headings_dict: dict):
        """Add a table of contents to the markdown report.

        Args:
            headings_dict (dict): A dictionary of headings where the key is the heading name and the
                value is a nested dictionary of subheadings. E.g.
                headings_dict = {
                    "Introduction": None,
                    "Data Sources": {
                        "data_source_1": None,
                        "data_source_2": None
                    }
                }
        """
        toc_markdown, toc_anchors = self._generate_table_of_contents(headings_dict)

        self.report.append(toc_markdown)

        return toc_anchors

    def _generate_table_of_contents(
        self,
        headings_dict: dict,
        toc_markdown: str = "",
        toc_anchors: dict = dict(),
        level: int = 0,
    ):
        """A (too) simple toc generator which can fail if any headings have the same name.

        Args:
            headings_dict (dict): A dictionary of headings where the key is the heading name and the
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
            toc_anchors (dict, optional): A dictionary of anchors which will be used to define the
                anchors in the markdown document, e.g. <a name="foo"></a>. It should not be set
                manually. Defaults to dict().
            level (int, optional): The level of indent for the toc. Defaults to 0.

        Returns:
            tuple: The markdown string for the toc and the dictionary of anchors.
        """
        index = 1

        for heading in headings_dict:
            anchor = heading.lower().replace("/", "_").replace(" ", "-")

            toc_markdown += f"{'    ' * level}{index}. [{heading}](#{anchor})\n"

            # Generate a unique anchor for the heading
            assert toc_anchors.get(heading) is None, "This heading already exists, use unique names for headings (or write a better toc \
                generator)."

            toc_anchors[heading] = anchor

            # Recursively cycle through subheadings
            subheadings_dict = headings_dict[heading]
            if subheadings_dict:
                toc_markdown, toc_anchors = self._generate_table_of_contents(
                    subheadings_dict, toc_markdown, toc_anchors, level=level + 1
                )

            index += 1

        return toc_markdown, toc_anchors

    def save(self, filepath: str):
        """Save the markdown report to the given filepath.

        Args:
        filepath (str): The filepath to save the markdown report to.
        """
        with open(filepath, "w", encoding="utf-8") as f:
            f.write("\n\n".join(self.report))


class DataSource:
    """A class for managing sources of data for the data extract.

    The sources are read from SQL or Table Storage and then stored in dataframes. From there we clean and pseudonomise the columns. Various sources can be combined to create extracts.
    """

    def __init__(
        self,
        name: str,
        dataframe: pd.DataFrame,
        parents: list = None,
        glossary: dict = dict(),
        columns_to_entities: dict = dict(),
        whitelist: set = set(),
        data_extract=None,
        description: string = None,
    ):
        """Initialisation.

        Args:
            name (str): The name of the data source.
            dataframe (pd.DataFrame): The dataframe containing the data.
            parents (list, optional): A list of parent data sources. Defaults to None.
            glossary (dict, optional): Definitions for the columns. Defaults to dict().
            columns_to_entities (dict, optional): A dictionary of column names to entity names. For example a column
                "CorrectionsQuizSessionId" would be mapped to the entity "QuizSessionId". Defaults to dict().
            whitelist (set, optional): A set of all the other columns which may look like ids but do not need to be
                processed. Defaults to set().
            data_extract (DataExtract, optional): The data extract which contains this data source. Defaults to None.
            description (string, optional): A description of the data source. Defaults to None.
        """
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
    def fromMerge(
        cls, name: str, parent_data_sources: list, merge_func: Callable, **kwargs
    ):
        """Class method for creating a data source from a merge of other data sources.

        Args:
            name (str): The name of the data source.
            parent_data_sources (list): A list of data sources to merge.
            merge_func (function): A function which takes a list of dataframes and returns a single dataframe.

        Returns:
            DataSource: An instance of the DataSource class.
        """
        dataframe = merge_func(parent_data_sources)

        return cls(
            name=name, dataframe=dataframe, parents=parent_data_sources, **kwargs
        )

    @classmethod
    def fromSQL(
        cls,
        name: str,
        sql: str,
        connection_string_name: str = "AZURE_SQL_REPORT_CONNECTION_STRING",
        **kwargs,
    ):
        """Class method for creating a data source by extracting data from a SQL database.

        Args:
            name (str): The name of the data source.
            sql (str): The SQL query to run on the database.
            connection_string_name (str, optional): Name of the environment variable which stores the connections string. Defaults to "AZURE_SQL_REPORT_CONNECTION_STRING".

        Returns:
            DataSource: An instance of the DataSource class.
        """
        cursor = _SQLconnection(connection_string_name)

        cursor.execute(sql)

        columns = [d[0] for d in cursor.description]
        rows = [list(i) for i in cursor.fetchall()]
        dataframe = pd.DataFrame(rows, columns=columns)

        return cls(name=name, dataframe=dataframe, **kwargs)

    @classmethod
    def fromSQLFile(
        cls,
        name: str,
        filename: str,
        connection_string_name: str = "AZURE_SQL_REPORT_CONNECTION_STRING",
        **kwargs,
    ):
        """Class method for creating a data source by extracting data from a SQL database.

        Args:
            name (str): The name of the data source.
            filename (str): The file containing the SQL query to run on the database.
            connection_string_name (str, optional): Name of the environment variable which stores the connections string. Defaults to "AZURE_SQL_REPORT_CONNECTION_STRING".

        Returns:
            DataSource: An instance of the DataSource class.
        """
        cursor = _SQLconnection(connection_string_name)

        with open(filename, "r") as file:
            sql = file.read()

        cursor.execute(sql)

        columns = [d[0] for d in cursor.description]
        rows = [list(i) for i in cursor.fetchall()]
        dataframe = pd.DataFrame(rows, columns=columns)

        return cls(name=name, dataframe=dataframe, **kwargs)

    @classmethod
    def fromTableStorage(
        cls,
        name: str,
        table_name: str,
        query_filter: str,
        connection_string_name: str,
        **kwargs,
    ):
        """Class method for creating a data source by extracting data from a Table Storage database.

        Args:
            name (str): The name of the data source.
            table_name (str): The name of the table storage table to query.
            query_filter (str): The query filter to apply to the table storage table.
            connection_string_name (str): Name of the environment variable which stores the connections string.

        Returns:
            DataSource: An instance of the DataSource class.
        """
        connection_string = os.getenv(connection_string_name)
        table_service_client = TableServiceClient.from_connection_string(
            conn_str=connection_string
        )
        table_client = table_service_client.get_table_client(table_name=table_name)

        entities = table_client.query_entities(query_filter)
        dataframe = pd.DataFrame(entities)

        return cls(name=name, dataframe=dataframe, **kwargs)

    @classmethod
    def fromCSV(cls, name: str, csv_path: Path | str, **kwargs):
        """Class method for creating a data source by reading in a CSV file.

        Args:
            name (str): The name of the data source.
            csv_path (Path | str): The path to the csv file.

        Returns:
            DataSource: An instance of the DataSource class.
        """
        dataframe = pd.read_csv(csv_path)

        return cls(name=name, dataframe=dataframe, **kwargs)

    def set_data_extract(self, data_extract):
        """Set the data extract for the data source.

        Args:
            data_extract (DataExtract): The data extract to set.
        """
        self.data_extract = data_extract

    def export(self, export_path: Path | str = None):
        """Export the data source as a csv.

        The intention is that this can be shared with others and should therefore not contain any sensitive information.

        Args:
            export_path (Path | str, optional): The path to the folder where the csv should be saved. Typically this will come from the
                data extract and be the following format:

                (self.data_extract_path / self.public_folder / "data")

                A csv with the name of the data source will be saved to this folder. Defaults to None.
        """
        assert (
            self.is_pseudonomised is True
        ), "You can only export data which has been pseudonomised."

        if export_path is None and self.data_extract:
            export_path = (
                self.data_extract.data_extract_path
                / self.data_extract.public_folder
                / "data"
            )

        if isinstance(export_path, str):
            export_path = Path(export_path)

        self.df.to_csv(str(export_path / f"{self.name}.csv"), index=False)

    def update_glossary(self, d):
        """Update the glossary with additional fields. Existing entries will be overwritten."""
        self.glossary = dict(self.glossary, **d)

    def _get_summary_statistics_field(self, field):
        def _get_common_stats(field):
            return {
                "field": field.name,
                "count": field.count(),
                "nunique": field.nunique(),
                "non-null": field.notnull().sum(),
            }

        def _handle_int_stats(field):
            stats = _get_common_stats(field)
            stats.update(
                {
                    "type": "int",
                    "min": field.min(),
                    "max": field.max(),
                }
            )

            if stats["nunique"] < 10:
                stats["value_counts"] = str(field.value_counts().to_dict())

            return stats

        def _handle_float_stats(field):
            # Check whether all non-null values are integers
            if field.dropna().apply(float.is_integer).all():
                return _handle_int_stats(field)

            stats = _get_common_stats(field)
            stats.update(
                {
                    "type": "float",
                    "min": field.min(),
                    "25%": field.quantile(0.25),
                    "50%": field.quantile(0.50),
                    "75%": field.quantile(0.75),
                    "max": field.max(),
                }
            )

            return stats

        def _handle_bool_stats(field):
            stats = _get_common_stats(field)
            stats.update(
                {"type": "bool", "value_counts": str(field.value_counts().to_dict())}
            )

            return stats

        def _handle_datetime_stats(field):
            stats = _get_common_stats(field)
            stats.update({"type": "datetime", "min": field.min(), "max": field.max()})

            return stats

        def _handle_str_stats(field):
            stats = _get_common_stats(field)
            stats.update(
                {
                    "type": "object",
                    "min_length": field.str.len().min(),
                    "max_length": field.str.len().max(),
                    "min_words": field.str.count(" ").min() + 1,
                    "max_words": field.str.count(" ").max() + 1,
                }
            )

            if stats["nunique"] < 10:
                stats["value_counts"] = str(field.value_counts().to_dict())

            return stats

        def _handle_default(field):
            stats = _get_common_stats(field)
            stats.update({"type": "unknown"})

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
        """Get summary statistics for the data source."""
        stats = []
        for field in self.df:
            stats.append(self._get_summary_statistics_field(self.df[field]))

        return pd.DataFrame(stats)

    def add_markdown_report(
        self, markdown_report: MarkdownReport, anchor: str, docs_path: Path
    ):  # noqa: C901
        """Add a markdown report for the data source to the given markdown report."""
        print(f"Generating statistics and markdown report for {self.name}...")

        markdown_report.add_heading(self.name, 3, anchor)

        if self.description:
            markdown_report.add_markdown(self.description)

        markdown_report.add_heading("Glossary", 4)

        if self.glossary:
            formatted_glossary = {
                k: v.replace("\n\n", "<br><br>") for k, v in self.glossary.items()
            }
            markdown_report.add_definitions(formatted_glossary)

        markdown_report.add_heading("Statistics", 4)

        summary_statistics = self.get_summary_statistics()

        markdown_report.add_dataframe(summary_statistics)

        markdown_report.add_heading("Sample", 4)

        if self.df.shape[0] > 0:
            markdown_report.add_dataframe(self.df.sample(3))


class DataExtract:
    """A class for managing data extracts."""

    def __init__(
        self, name: str, description: str, container_path: Path | str = "data"
    ):
        """Initialisation.

        Args:
            name (str): A short human-friendly name for the data extract.
            description (str): A longer explanation of what this particular extract is, why it was created, who it is
                for, etc.
            container_path (Path | str, optional): The path to the folder which contains all data extracts. Defaults to "data".
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

    @classmethod
    def fromPickle(cls, filepath: Path | str) -> "DataExtract":
        """Load a data extract from the given filepath.

        Args:
            filepath (Path | str): The path to the data extract.

        Usage:
        d = DataExtract.fromPickle(".foo/bar.pkl")  # Using a string
        or
        d = DataExtract.fromPickle(Path(".foo/bar.pkl"))  # Using a Path object
        """
        with set_posix_windows():
            with open(filepath, "rb") as f:
                p = pickle.load(f)

        assert isinstance(p, cls), "The object is not a DataExtract."

        return p

    def __str__(self):
        """Return a string representation of the data extract."""
        return (
            f"Name:\t\t{self.name}\n"
            + f"Id:\t\t{self.id}\n"
            + f"Datetime:\t{self.datetime}\n"
            + f"Description:\t{self.description}"
        )

    def __stamp(self):
        """Stamps the data extract with a unique code and datetime."""
        size = 32
        chars = string.ascii_lowercase + string.digits

        self.id = "".join(random.choices(chars, k=size))
        self.datetime = datetime.now(timezone.utc)

    def set_paths_and_filenames(self):
        """Sets the path and filenames for the data extract.

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
        self.name_date_id = (
            f"{self.name}-{self.datetime.strftime('%Y%m%d%H%M')}-{self.id}"
        )
        self.data_extract_path = self.container_path / self.name_date_id
        self.public_folder = f"{self.name_date_id}-PUBLIC"
        self.public_archive_filename = f"{self.name_date_id}-PUBLIC.zip"
        self.private_filename = f"{self.name_date_id}-PRIVATE.pkl"

        (self.data_extract_path / self.public_folder / "data").mkdir(
            parents=True, exist_ok=True
        )
        (self.data_extract_path / self.public_folder / "img").mkdir(
            parents=True, exist_ok=True
        )
        (self.data_extract_path / self.public_folder / "docs" / "img").mkdir(
            parents=True, exist_ok=True
        )

    def add_data_source(self, data_source: DataSource):
        """Add a data source to the data extract.

        Args:
            data_source (DataSource): The data source to add.
        """
        self.data_sources[data_source.name] = data_source
        data_source.set_data_extract(self)

    def get_or_set_data_source(
        self, data_source_name: str, data_source_func: Callable, **kwargs
    ) -> DataSource:
        """Get or set a data source.

        Args:
            data_source_name (str): The name of the data source.
            data_source_func (Callable): A function which takes a name and returns a data source.
        """
        ds = self.data_sources.get(data_source_name, None)

        if ds is None:
            ds = data_source_func(data_source_name, **kwargs)
            self.add_data_source(ds)

        return ds

    def get_or_set_data_source_class(
        self, DataSourceClass: type[DataSource], **kwargs
    ) -> DataSource:
        """Get or set a data source.

        Args:
            DataSourceClass (DataSource): The data source class.
        """
        ds = self.data_sources.get(DataSourceClass._name, None)

        if ds is None:
            ds = DataSourceClass(**kwargs)
            self.add_data_source(ds)

        return ds

    def save(self) -> None:
        """Save the data extract."""
        filepath = str(self.data_extract_path / self.private_filename)

        with open(filepath, "wb") as file:
            pickle.dump(self, file)

    def export(self, data_source_names: list[str] = None):
        """Export the data extract as a csv.

        The intention is that this can be shared with others and should therefore not contain any sensitive information.

        Args:
            data_source_names (list[str], optional): A list of data source names to export. If None, then all data sources will be exported.
        """
        if data_source_names is None:
            data_source_names = self.data_sources.keys()

        for name in data_source_names:
            ds = self.data_sources[name]
            ds.export()

    def archive(self, container_name: str):
        """Archive and upload the data extract to Azure Blob Storage.

        Args:
        container_name (str): The name of the Azure Blob Storage container to upload the data extract to.
        """
        folder_to_archive_path = self.data_extract_path / self.public_folder
        archive_file_path = self.data_extract_path / self.public_archive_filename

        zip_folder(folder_to_archive_path, archive_file_path)

        blob = Blobs(container_name)
        blob.upload_file(archive_file_path)

    def find_id_columns(self):
        """Find all the columns which look like ids."""
        return {i for i in self.df.columns if re.search(r"[i|I][d|D]$", i)}

    def pseudonomise(self, data_source_name: str):
        """Changes the values in the specified dataframe columns so they cannot be linked to our database.

        Args:
            data_source_name (str): The name of the data source to process.
        """
        data_source = self.data_sources.get(data_source_name, None)

        assert data_source is not None, "The data source name is not found."

        assert not (
            data_source.is_pseudonomised
        ), "This data frame has already been pseudonomised."

        dataframe = data_source.df
        columns_to_entities = data_source.columns_to_entities
        whitelist = data_source.whitelist

        id_columns = {i for i in dataframe.columns if re.search(r"[i|I][d|D]$", i)}

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

    def __real_id_to_pseudo_id(
        self, real_ids: pd.Series, real_id_to_pseudo_id: dict = {}
    ):
        """Returns dictionary mapping real ids to pseudo ids.

        Args:
            real_ids (pd.Series) : A series of integer values we want to map to pseudo ids.
            real_id_to_pseudo_id (dict, optional) : A dictionary mapping real_ids to pseudo_ids. If present then update.

        Returns:
            dict: A dictionary mapping read ids to pseudo ids.
        """
        real_ids = real_ids.dropna()
        real_ids = real_ids.drop_duplicates()
        real_ids = real_ids.astype(int)

        existing_real_ids = real_id_to_pseudo_id.keys()

        new_real_ids = real_ids[~real_ids.isin(existing_real_ids)]

        start_pseudo_id = (
            max(real_id_to_pseudo_id.values()) + 1 if real_id_to_pseudo_id != {} else 0
        )

        new_real_id_to_pseudo_id = {
            new_real_id: i + start_pseudo_id
            for i, new_real_id in enumerate(new_real_ids)
        }

        real_id_to_pseudo_id = {**real_id_to_pseudo_id, **new_real_id_to_pseudo_id}

        assert len(real_id_to_pseudo_id.keys()) == len(
            set(real_id_to_pseudo_id.values())
        ), "At least two keys are mapped to the same value."

        return real_id_to_pseudo_id

    def generate_markdown_report(self) -> None:
        """Generate a markdown report for the data extract, reporting statistics on each data source."""
        docs_path = self.data_extract_path / self.public_folder / "docs"

        markdown_report = MarkdownReport(f"Data analysis for {self.name}")

        # Add a table of contents
        table_of_contents = {
            "Introduction": None,
            "Data Sources": {k: None for k in self.data_sources.keys()},
        }

        anchors = markdown_report.add_table_of_contents(table_of_contents)

        # Add some high level data extract details.
        markdown_report.add_definitions(
            [["Name", self.name], ["Id", self.id], ["Datetime", self.datetime]]
        )

        # Add an introduction
        markdown_report.add_heading(
            "Introduction", level=1, anchor=anchors["Introduction"]
        )
        markdown_report.add_markdown(self.description)

        # Add the data sources
        markdown_report.add_heading(
            "Data Sources", level=1, anchor=anchors["Data Sources"]
        )
        for k, v in self.data_sources.items():
            v.add_markdown_report(markdown_report, anchors[k], docs_path)

        # Save the report
        markdown_report.save(docs_path / "README.md")

    @staticmethod
    def get_data_source_from_list(
        name: str, data_sources: list[DataSource]
    ) -> DataSource:
        """Get a data source from a list of data sources given the name.

        Args:
            name (str): The name of the data source to get.
            data_sources (list[DataSource]): The list of data sources to look in.

        Returns:
            DataSource: The data source retreived.
        """
        try:
            return next(x for x in data_sources if x.name == name)
        except StopIteration:
            print(f"There is no data source with the name {name}.")
