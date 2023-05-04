# Eedata

### python package to manipulate data from database and blob stroage

1. create your virtual environment with venv of your choice. you can use python >3.8
    
    ```bash
    conda create -n [venv] python=3.9
    ```
    
2. clone this repository
    
    ```bash
    git clone git@github.com:Eedi/eedata.git
    ```
    
3. move to `eedata` directory and install this package as an editable package
    
    ```bash
    pip install -e .
    ```
    
4. you would need `.env` and .`sheets` file. contact @Maryam Honari or @Simon Woodhead to get them
5. you can now import this package:
    
    ```python
    from eedata.helper_functions import fromSQL, fromGSheet
    ```
