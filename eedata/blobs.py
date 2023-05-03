# https://docs.microsoft.com/en-us/azure/architecture/data-science-process/explore-data-blob
# https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python

import errno
import os

from azure.storage.blob import BlobServiceClient


class Blobs:
    """
    A class for managing blob upload and download.

    In order for this to work you may need to 'pip install azure-storage-blob'.

    Attributes
    ----------
    repo_name : str
        The name of the repository which is used as the blob container name.

    Methods
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
        self, repo_name, connection_string="AZURE_STORAGE_EEDIDATA_CONNECTION_STRING"
    ):
        """initialisation

        Parameters
        ----------
        repo_name : str
            The name of the repository which is used as the blob container name.
        """

        self.__set_blob_service_client(connection_string)

        self.__get_or_set_container(repo_name)

    def __set_blob_service_client(self, connection_string):
        """The connection string is set as an environment variable on the user's computer.
        You can set this value using the following code:

        Windows:   setx AZURE_STORAGE_EEDIDATA_CONNECTION_STRING "<yourconnectionstring>"
        Linux/Mac: export AZURE_STORAGE_EEDIDATA_CONNECTION_STRING="<yourconnectionstring>"

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

    def upload(self, filename, overwrite=False):
        """Upload a local file to the container.

        Parameters
        ----------
        filename : str
            The name of the file to upload.

        overwrite : bool, optional
            True = overwrite, False = do not overwrite.

        Raises
        ------
        FileNotFoundError
            If `filename` does not exist locally.
        """

        local_file_path = os.path.join(self.local_path, filename)

        print("\nUploading blob from \n\t" + local_file_path)

        if not os.path.exists(local_file_path):
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), filename)

        # Create a blob client using the local file name as the name for the blob
        blob_client = self.container_client.get_blob_client(blob=filename)

        with open(local_file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=overwrite)

    def download(self, filename):
        """Download a file from the container.

        Parameters
        ----------
        filename : str
            The name of the file to download.

        Raises
        ------
        FileNotFoundError
            If `filename` does not exist in the container.
        """

        local_file_path = os.path.join(self.local_path, filename)

        print("\nDownloading blob to \n\t" + local_file_path)

        blob_client = self.container_client.get_blob_client(blob=filename)

        if not blob_client.exists():
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), filename)

        with open(local_file_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())

    def delete(self, filename):
        """Delete a file from the container.

        Parameters
        ----------
        filename : str
            The name of the file to delete.

        Raises
        ------
        FileNotFoundError
            If `filename` does not exist in the container.
        """

        blob_client = self.container_client.get_blob_client(blob=filename)

        if not blob_client.exists():
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), filename)
        print(f"Are you sure you want to delete {filename}, Y or N")

        x = input()
        if x in ["Y", "y", "Yes", "yes"]:
            self.container_client.delete_blob(blob=filename)
            print(f"{filename} has been deleted")
        else:
            print(f"{filename} has not been deleted")

    def compare_files(self, localpath="data"):
        """Compare filenames in local storage and blob storage.

        Parameters
        ----------
        localpath : str
            The name of local path where files are stored.

        Raises
        ------
        FileNotFoundError
            If `local path` does not exist.
        """

        blob_files = {x.name for x in self.blobs_list()}
        local_files = {y for y in os.listdir(localpath) if not y.startswith(".")}

        local_missing = blob_files - local_files
        if local_missing == set():
            print("All BLOB files are on your LOCAL machine.")
        if not local_missing == set():
            print(
                f"These BLOB files are missing from your LOCAL machine: \n{local_missing}"
            )
            print(
                f'\nTo download all use:\n.sync(direction="download", which="missing", choose=False, localpath="data")'
            )

        print("\n##########\n")

        blob_missing = local_files - blob_files
        if blob_missing == set():
            print("All LOCAL files are in BLOB storage.")
        if not blob_missing == set():
            print(f"These LOCAL files are missing from BLOB storage: \n{blob_missing}")
            print(
                f'\nTo upload all use:\n.sync(direction="upload", which="missing", choose=False, localpath="data")'
            )

        print("\n##########\n")

        in_both = blob_files.intersection(local_files)
        if in_both == set():
            print("No files have a copy in both BLOB and LOCAL storage.")
        if not in_both == set():
            print(
                f"These files have a copy in both BLOB and LOCAL storage: \n{in_both}"
            )

    def sync(self, direction="", which="missing", choose=False, localpath="data"):
        """Download and upload batches of files between local and blob storage.

        Parameters
        ----------
        direction : str
            upload = upload from local to blob storage.
            download = download from blob to local storage.

        which : str, default is 'missing'
            missing = download or upload files not present in both local and blob storage.
            existing = download or upload files that are present in both local and blob storage.
            all = download or upload all files.

        choose : bool, default is False
            False = download or upload files without human input.
            True = human input required to choose which files to download or upload

        localpath : str
            The local filepath

        Raises
        ------
        ERROR
            If incorrect keyword used.

        FileNotFoundError
            If `local path` does not exist.
        """

        # Keyword validation.
        if (
            (which not in ["missing", "existing", "all"])
            or (choose not in [True, False])
            or (direction not in ["upload", "download"])
        ):
            return print(
                "ERROR:  Incorrect keyword assignment.\n\
            direction must be, 'upload' or 'download' \n\
            which must be 'missing', 'existing' or 'all'.\n\
            choose must be True or False."
            )

        # Run function twice if keyword "all" used.
        if which == "all":
            self.sync(
                direction=direction,
                which="existing",
                choose=choose,
                localpath=localpath,
            )
            self.sync(
                direction=direction, which="missing", choose=choose, localpath=localpath
            )
            return

        # Find blob and local files
        blob_files = {x.name for x in self.blobs_list()}
        local_files = {y for y in os.listdir(localpath) if not y.startswith(".")}

        # Assigning which files to sync
        if which == "existing":
            files = local_files.intersection(blob_files)

        if direction == "upload" and which == "missing":
            files = local_files - blob_files

        if direction == "download" and which == "missing":
            files = blob_files - local_files

        # Behaviour if no files are found.
        if files == set():
            return print(f"There are no {which} files to {direction}.")

        # Behaviour if files are found
        if not files == set():
            call_dict = {
                "upload": lambda w: self.upload(w, overwrite=True),
                "download": self.download,
            }

            if choose == False:
                print(f"\nAbout to {direction} the {which} files.")
                list(map(lambda x: call_dict[direction](x), files))

            elif choose == True:
                print(f"Choose the {which} files you want to {direction}.")
                for i in files:
                    choice = input(
                        f"Would you like to {direction} the {which} file: {i}?"
                    )
                    if choice.lower() in {"yes", "y", "yeah"}:
                        call_dict[direction](i)
                    else:
                        print(f"\n{i} not {direction}ed")
