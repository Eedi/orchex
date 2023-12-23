# Orchex

`Orchex` is a library for the orchestration of data workflows, including hierarchical extraction, transformation with pseudonymisation, automated documentation, and secure sharing mechanisms.

## Installation

`orchex` uses `poetry` (do not use `pip` or `conda`). To create the environment run:

```bash
poetry install
```

## Using `orchex` in other repositories

Previously we would have installed the package globally using `pip install -e .`, using `poetry` you simply add a dependency to the local package.

1. Clone the repository:
    
    ```bash
    git clone git@github.com:Eedi/eedata.git
    ```
    
2. In your other repository:

    ```bash
    poetry add path/to/this/package
    ```
3. Some environments variables (`.env` and `.sheets`) are required for some components. Contact [Simon](simon.woodhead@eedi.co.uk) for details.

4. You can now import this package:
    
    ```python
    from orchex.dataextract import DataExtract, DataSource
    ```
