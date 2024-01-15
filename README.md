## Table of Contents
1. [Overview](#overview) 📖
2. [Setup](#setup) 🧑‍🔬
    * [Requirements](#req) 📋
3. [Installation](#installation) 
    * Windows (#windows)
    * MacOS (#mac)
    * Linux (#linux)
4. [Using `orchex` in other repositories](#otherRepo) 


# Orchex <a id="overview"></a>

`Orchex` is a library for the orchestration of data workflows, including hierarchical extraction, transformation with pseudonymisation, automated documentation, and secure sharing mechanisms.


# Setup <a id="setup"></a>
## Requirements <a id="req"></a>

#### Python
`Python version 3.12^` is required


#### `.env` file
To extract data from the database some azure specific variables are required to be stored in a `.env` file. If you don't have those information please contact [Simon](mailto:simon.woodhead@eedi.co.uk)

## Installation <a id="installation"></a>

### Poetry
`orchex` uses `poetry` (do not use `pip` or `conda`).
* To create the environment:

#### Windows <a id="windows"></a>

```bash
poetry env use 3.12
poetry install
```

* To activate: 

```bash
poetry shell
```

#### MacOS <a id="mac"></a>

* To create the environment:
```bash
poetry env use 3.12
poetry config --local installer.no-binary pymssql

brew install FreeTDS
export CFLAGS="-I$(brew --prefix openssl)/include"
export LDFLAGS="-L$(brew --prefix openssl)/lib -L/usr/local/opt/openssl/lib"
export CPPFLAGS="-I$(brew --prefix openssl)/include"

poetry install
```

* To activate: 

```bash
poetry shell
```



#### Linux/ Eedi VM <a id="linux"></a>

* To create the environment:

```bash
export PYTHON_KEYRING_BACKEND=keyring.backends.fail.Keyring

poetry env use 3.12
poetry config --local installer.no-binary pymssql

poetry install
poetry shell

```
NOTE: if you get the following error :
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
