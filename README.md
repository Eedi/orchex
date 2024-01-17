## Table of Contents
1. [Overview](#overview) 📖
2. [Setup](#setup) 🧑‍🔬
    * [Prerequisites](#prereq) 📋
    * [Installation](#installation) ⏬
        * [Windows](#windows)
        * [MacOS](#mac)
        * [Linux](#linux)
4. [Using `orchex` in other repositories](#otherRepo) 


# Orchex <a id="overview"></a> 📖

On a snapshot, `orchex` is a library for the orchestration of data workflows, including hierarchical extraction, transformation with pseudonymisation, automated documentation, and secure sharing mechanisms.

In more detail, the main code of the module can be found under `/orchex/dataextract.py` where the code for the two main data classes `DataSource` and `DataExtract` can be found.

* `DataSource`

    This class contains several methods that allow the user to extract data from a data source 
    and create a dataframe object. Supported data sources: 
    * SQL code
    * SQL file
    * Table Storage database 
    * csv file

* `DataExtract`

    This class allows the user to combine multiple `DataSources` objects at a single object. This facilitates easy execution of the same operation to multiple different `DataSources` such as pseudonymisation. 

    The data will be stored in the following  filestructue 

    ```
    {name}-{YYYYmmDDHHMM}-{id}
    ├── {name}-{YYYYmmDDHHMM}-{id}-PRIVATE.pkl
    ├── {name}-{YYYYmmDDHHMM}-{id}-PUBLIC/
    │   ├── data
    │   │   └──{data_source_name}.csv
    │   ├──img
    │   ├──docs
    │       ├── README.md
    │       └── img
    ```

    This class allows for 3 different ways of saving the data:

    * `save()`: _saves a `.pkl` file of the class. Recommended for personal use. __NOT__ sharing data_

    * `export()`: _creates pseudonymised `.csv` files. Best way to share data_
    * `archive()`: _creates a `.zip` file with all the created folders and uploads them to Azure Blob Storage._

📝
__NOTE__: In both classes there is the functionality to create a markdown report with all the class info.


# 2. Setup <a id="setup"></a> 🧑‍🔬
## 2.1 Prerequisites <a id="prereq"></a> 📋

* ### Python 🐍
    `Python version 3.12^` is required

* ### ODBC Driver (if running SQL code) 💻
    If you wish to create `DataSources` and/or `DataExtracts` using SQL code then, ODBC drivers should be installed.
    Please follow the instructions on the following page based on your OS (v17+ is recommended):

    * [Windows](https://learn.microsoft.com/en-au/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-2017#download-for-windows)
    * [MacOS](https://learn.microsoft.com/en-au/sql/connect/odbc/linux-mac/install-microsoft-odbc-driver-sql-server-macos?view=sql-server-2017)
    * [Linux](https://learn.microsoft.com/en-au/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-2017&tabs=alpine18-install%2Calpine17-install%2Cdebian8-install%2Credhat7-13-install%2Crhel7-offline) 

* ### `.env` file 📃
    To extract data from the database some azure specific variables are required to be stored in a `.env` file. If you don't have those information please contact [Simon](mailto:simon.woodhead@eedi.co.uk)

## 2.2 Installation <a id="installation"></a>  ⏬

### Poetry 
`orchex` uses `poetry` __(do not use `pip` or `conda`)__.
To create the environment:

* #### Windows <a id="windows"></a>

    ```shell
    poetry env use 3.12
    poetry install

    # to activate the env
    poetry shell
    ```


* #### MacOS <a id="mac"></a>


    ```bash
    poetry env use 3.12
    poetry config --local installer.no-binary pymssql

    brew install FreeTDS
    export CFLAGS="-I$(brew --prefix openssl)/include"
    export LDFLAGS="-L$(brew --prefix openssl)/lib -L/usr/local/opt/openssl/lib"
    export CPPFLAGS="-I$(brew --prefix openssl)/include"

    poetry install

    # to activate the env
    poetry shell
    ```

* #### Linux/ Eedi VM <a id="linux"></a>


    ```bash
    export PYTHON_KEYRING_BACKEND=keyring.backends.fail.Keyring

    poetry env use 3.12
    poetry config --local installer.no-binary pymssql

    poetry install

    # to activate the env
    poetry shell
    ```

    ❗ __NOTE__:
    if you get the following error 

    ```shell
    This error originates from the build backend, and is likely not a problem with poetry but with multidict (6.0.4) not supporting PEP 517 builds. You can verify this by running 'pip wheel --use-pep517 "multidict (==6.0.4)"'.
    ```
    Run:

    ```shell
    poetry shell
    pip install --upgrade pip
    MULTIDICT_NO_EXTENSIONS=1 pip install multidict
    poetry add inflect
    poetry add pyodbc

    # if package are not reinstalled then run: 
    poetry update
    ```


## Using `orchex` in other repositories <a id=otherRepo></a>

Previously we would have installed the package globally using `pip install -e .`, using `poetry` you simply add a dependency to the local package.

1. Clone the repository:
    
    ```bash
    git clone git@github.com:Eedi/orchex.git
    ```
    
2. In your other repository, add the following to the `pyproject.toml`:

    ```
    orchex = {path = "../orchex", develop = true}
    ```
    The develop flag should mean that your installation will be automatically updated when `orchex` is editted.

3. Some environments variables (`.env` and `.sheets`) are required for some components. Contact [Simon](mailto:simon.woodhead@eedi.co.uk) for details.

4. You can now import this package:
    
    ```python
    from orchex.dataextract import DataExtract, DataSource
    ```

5. If you then update this package it should update automatically (if `develop = true`). If this does not happen you should be able to just run `poetry update orchex` but you may need to reinstall your poetry environment. To do so:

    - Close any IDEs (i.e. VS Code) that might be using the environment. (Otherwise the following will fail.)
    - Run `poetry env list` to get the name of the environment.
    - Remove the environment ```poetry env remove orchex-fYa19ibp-py3.12```
    - Go and delete where the environment folder is (e.g. `E:\packages\poetry\virtualenvs`). This is necessary otherwise the next step will just reinstalled some cached versions.
    - Reinstall ```poetry install```
    - In VS Code you may need to manually select the new environment. `Ctrl-Shift-P`, then click `Enter interpreter path...`.
