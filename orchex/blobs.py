"""A class for managing blob upload and download."""

import errno
import os
from collections import namedtuple
from datetime import datetime, timedelta
from pathlib import Path
from typing import Set

from azure.storage.blob import BlobSasPermissions, BlobServiceClient, generate_blob_sas

FileSyncInfo = namedtuple(
    "FileSyncInfo",
    ["local_files", "blob_files", "local_missing", "blob_missing", "in_both"],
)


class Blobs:
    """A class for managing blob upload and download.

    In order for this to work you may need to 'pip install azure-storage-blob'.

    Attributes:
    ----------
    container_name : str
        The name for the blob container, typically we use the repository name.

    Methods:
    -------
    blobs_list()
        Prints a list of the blobs within the container.

    upload(filename, overwrite=False)
        Upload a local file to the container.

    download(filename)
        Download a file from the container to your local space.

    delete(filename)
        Delete a file within the container.
    """

    local_path = "data"

    def __init__(
        self,
        container_name,
        connection_string="AZURE_STORAGE_EEDIDATA_CONNECTION_STRING",
    ):
        """Initialisation.

        Parameters
        ----------
        container_name : str
            The name for the blob container.
        """
        self.__set_blob_service_client(connection_string)

        self.__get_or_set_container(container_name)

    def __set_blob_service_client(self, connection_string):
        """The connection string is set as an environment variable on the user's computer.

        You can set this value using the following code:

        Windows:   setx AZURE_STORAGE_EEDIDATA_CONNECTION_STRING "<yourconnectionstring>"
        Linux/Mac: export AZURE_STORAGE_EEDIDATA_CONNECTION_STRING="<yourconnectionstring>"

        However, it is more convenient to use a .env file together with dotenv.

        The connection string comes from the Azure Portal Storage Accounts under Access Keys.
        """
        connect_str = os.getenv(connection_string)
        self.blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    def __get_or_set_container(self, container_name):
        """Try to get the container with a given `container_name`.

        If it doesn't exist then create it.

        Parameters
        ----------
        container_name : str
            The name to be used for the blob container.
        """
        self.container_client = self.blob_service_client.get_container_client(
            container_name
        )

        if not self.container_client.exists():
            self.container_client.create_container()

    def blobs_list(self):
        """Prints a list of the blobs within the container."""
        return list(self.container_client.list_blobs())

    def upload(
        self,
        filepath: Path | str,
        overwrite=False,
        container_path: Path | str | None = None,
    ):
        """Upload a local file to the container.

        Parameters
        ----------
        filepath : Path | str
            The path to the file you want to upload.

        overwrite : bool, optional
            True = overwrite, False = do not overwrite.

        container_path : Path | str, optional
            The path to a containing folder from which the relative path will be calculated to name
            the blob.

        Raises:
        ------
        FileNotFoundError
            If `filename` does not exist locally.
        """
        filepath = Path(filepath)

        # If no container is defined we will simply use the filename for the blob name.
        container_path = (
            Path(container_path) if container_path is not None else Path.cwd()
        )

        # The relative filepath is used as the name for the blob.
        relative_filepath = str(filepath.relative_to(container_path))

        print(f"\nUploading blob from: {filepath}")

        if not os.path.exists(filepath):
            raise FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), filepath.name
            )

        # Create a blob client using the relative filepath as the name for the blob
        blob_client = self.container_client.get_blob_client(blob=relative_filepath)

        with open(filepath, "rb") as data:
            blob_client.upload_blob(data, overwrite=overwrite)

        return blob_client

    def download(self, blobname: str, container_path: Path | str | None = None):
        """Download a blob from the container.

        Parameters
        ----------
        blobname : str
            The name of the blob to download. Note that blob names can contain folders.

        container_path : Path | str, optional
            The path to the folder which will contain the downloaded file.

        Raises:
        ------
        FileNotFoundError
            If `blobname` does not exist in the container.
        """
        # Set the local container to the current working directory if none is specified
        container_path = (
            Path(container_path) if container_path is not None else Path.cwd()
        )

        filepath = container_path / blobname

        # Blob names can contain folders, fix any Windows style slashes
        blobname = blobname.replace("\\", "/")

        print(f"Downloading blob to:\n{str(filepath)}")

        blob_client = self.container_client.get_blob_client(blob=blobname)

        if not blob_client.exists():
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), blobname)

        # Recursively create the folders if they don't exist
        filepath.parent.mkdir(parents=True, exist_ok=True)

        with open(filepath, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())

    def delete(self, blobname: str):
        """Delete a blob from the container.

        Parameters
        ----------
        blobname : str
            The name of the blob to delete.

        Raises:
        ------
        FileNotFoundError
            If `blobname` does not exist in the container.
        """
        # Blob names can contain folders, fix any Windows style slashes
        blobname = blobname.replace("\\", "/")

        blob_client = self.container_client.get_blob_client(blob=blobname)

        if not blob_client.exists():
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), blobname)

        print(f"Are you sure you want to delete {blobname}? (Y or N)")

        x = input()

        if x in ["Y", "y", "Yes", "yes"]:
            self.container_client.delete_blob(blob=blobname)
            print(f"{blobname} has been deleted")
        else:
            print(f"{blobname} has not been deleted")

    def get_blob_url_with_sas(self, blob_name: str, expiry_days: int = 28) -> str:
        """Generate a URL with a SAS token for the given blob name.

        Parameters
        ----------
        blob_name : str
            The name of the blob to generate the SAS token for.

        Returns:
        -------
        str
            The URL with a SAS token for the blob.
        """
        # Blob names can contain folders, fix any Windows style slashes
        blob_name = blob_name.replace("\\", "/")

        # Set the expiry time and permissions for the SAS token
        sas_token_expiry = datetime.utcnow() + timedelta(days=expiry_days)
        sas_start_time = datetime.utcnow() - timedelta(minutes=5)
        sas_permissions = BlobSasPermissions(read=True, write=False)

        container_name = self.container_client.container_name

        # Generate the SAS token
        sas_token = generate_blob_sas(
            account_name=self.blob_service_client.account_name,
            container_name=container_name,
            blob_name=blob_name,
            account_key=self.blob_service_client.credential.account_key,
            permission=sas_permissions,
            start=sas_start_time,
            expiry=sas_token_expiry,
        )

        # The SAS token includes multiple query parameters in a string so is it easier to form the
        # URL manually rather than using
        primary_endpoint = self.blob_service_client.primary_endpoint

        # Ensure the base URL ends with a slash
        if not primary_endpoint.endswith("/"):
            primary_endpoint += "/"

        # Construct the blob URL
        blob_url = f"{primary_endpoint}{container_name}/{blob_name}"

        # Add the query parameters to the URL
        blob_url_with_sas = f"{blob_url}?{sas_token}"

        return blob_url_with_sas

    def _get_file_sync_info(
        self, container_path: Path, extensions_to_include
    ) -> FileSyncInfo:
        """Get the local and remote files to compare.

        Parameters
        ----------
        container_path : Path
            The path to the local folder which contains all the files we want to compare to the
            remote blob container.

        extensions_to_include : Set[str]
            The file extensions to include.

        Returns:
        -------
        FileSyncInfo
            A named tuple containing: local files, blob files, files missing locally, files missing
            in the blob container, and files that exist both locally and in the blob
        """
        # Get all files recursively with specified extensions, excluding those that start with "_"
        local_files = {
            str(f.relative_to(container_path)).replace("\\", "/")
            for f in container_path.rglob("*")
            if f.is_file()
            and not f.name.startswith("_")
            and not f.name.startswith(".")
            and f.suffix in extensions_to_include
        }

        blob_files = {
            x.name
            for x in self.blobs_list()
            if x.name.endswith(tuple(extensions_to_include))
        }

        # Find any local missing files
        local_missing = blob_files - local_files

        # Find any blob missing files
        blob_missing = local_files - blob_files

        # Find files which exist locally and in the blob container
        in_both = local_files.intersection(blob_files)

        return FileSyncInfo(
            local_files, blob_files, local_missing, blob_missing, in_both
        )

    def diff(
        self,
        local_container_path: Path | str | None = None,
        extensions_to_include={".csv", ".xls", ".xlsx", ".zip"},
    ):
        """Compare local files with those in this blob container.

        Parameters
        ----------
        local_container_path : Path | str, optional
            The path to the local folder which contains all the files we want to compare to the
            remote blob container. If none is specified then the current working directory is used.

        extensions_to_include : Set[str], optional
            The file extensions to include. We typically only want to check data files so .csv,
            .xls, .xlsx, and .zip are included by default.
        """
        # Set the local container to the current working directory if none is specified
        local_container_path = (
            Path(local_container_path)
            if local_container_path is not None
            else Path.cwd()
        )

        fsi = self._get_file_sync_info(local_container_path, extensions_to_include)

        messages = []

        messages.append(
            "All blobs exist on your local machine."
            if fsi.local_missing == set()
            else f"These blobs are missing from your local machine: \n{fsi.local_missing}"
        )

        messages.append(
            "All local files exist in blob storage."
            if fsi.blob_missing == set()
            else f"These local files are missing from blob storage: \n{fsi.blob_missing}"
        )

        messages.append(
            "No files exist in both blob and local storage."
            if fsi.in_both == set()
            else f"These files exist in both blob and local storage: \n{fsi.in_both}"
        )

        print("\n\n".join(messages))

    def batch_download(
        self,
        container_path: Path | str | None = None,
        extensions_to_include={".csv", ".xls", ".xlsx", ".zip"},
        is_update_existing=True,
        is_add_missing=True,
        is_confirm=True,
    ):
        """Download a batch of files from blob storage to the local container path.

        Args:
            container_path (Path | str | None, optional): The path to the local folder which contains all the files we want to compare to the remote blob container. Defaults to None.
            extensions_to_include (set, optional): The types of file to include. Defaults to {".csv", ".xls", ".xlsx", ".zip"}.
            is_update_existing (bool, optional): Whether to update existing files. Defaults to True.
            is_add_missing (bool, optional): Whether to download files which are missing in the local folder. Defaults to True.
            is_confirm (bool, optional): Whether to ask the user to confirm each download. Defaults to True.
        """
        # Set the local container to the current working directory if none is specified
        container_path = (
            Path(container_path) if container_path is not None else Path.cwd()
        )

        fsi = self._get_file_sync_info(container_path, extensions_to_include)

        blobs_to_download: Set[str] = set()

        if is_update_existing:
            blobs_to_download = blobs_to_download.union(fsi.in_both)

        if is_add_missing:
            blobs_to_download = blobs_to_download.union(fsi.local_missing)

        if blobs_to_download == set():
            print("There are no files to download.")
        else:
            print("Starting to download the files.")

        for blob in blobs_to_download:
            if is_confirm:
                choice = input(f"Would you like to download the file: {blob}?")
                if choice.lower() in {"yes", "y"}:
                    self.download(blob, container_path)
                else:
                    print(f"\n{blob} not downloaded")
            else:
                self.download(blob, container_path)

    def batch_upload(
        self,
        container_path: Path | str | None = None,
        extensions_to_include={".csv", ".xls", ".xlsx", ".zip"},
        is_update_existing=True,
        is_add_missing=True,
        is_confirm=True,
        overwrite=True,
    ):
        """Upload a batch of files from local container path to blob storage.

        Args:
            container_path (Path | str | None, optional): The path to the local folder which contains all the files we want to compare to the remote blob container. Defaults to None.
            extensions_to_include (set, optional): The types of file to include. Defaults to {".csv", ".xls", ".xlsx", ".zip"}.
            is_update_existing (bool, optional): Whether to update existing files. Defaults to True.
            is_add_missing (bool, optional): Whether to download files which are missing in the local folder. Defaults to True.
            is_confirm (bool, optional): Whether to ask the user to confirm each download. Defaults to True.
            overwrite (bool, optional): Whether to overwrite files in blob storage if they already exist. Defaults to True.
        """
        # Set the local container to the current working directory if none is specified
        container_path = (
            Path(container_path) if container_path is not None else Path.cwd()
        )

        fsi = self._get_file_sync_info(container_path, extensions_to_include)

        files_to_upload: Set[str] = set()

        if is_update_existing:
            files_to_upload = files_to_upload.union(fsi.in_both)

        if is_add_missing:
            files_to_upload = files_to_upload.union(fsi.blob_missing)

        if files_to_upload == set():
            print("There are no files to upload.")
        else:
            print("Starting to upload the files.")

        for relative_filepath in files_to_upload:
            if is_confirm:
                choice = input(
                    f"Would you like to upload the file: {relative_filepath}?"
                )
                if choice.lower() in {"yes", "y"}:
                    absolute_filepath = container_path / relative_filepath
                    self.upload(absolute_filepath, overwrite, container_path)
                else:
                    print(f"\n{relative_filepath} not uploaded")
            else:
                self.upload(relative_filepath, overwrite, container_path)
