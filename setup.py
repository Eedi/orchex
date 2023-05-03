"""KAML setup script"""
from setuptools import find_packages, setup

setup(
    name="eedata",
    version="0.1.0",
    author="Maryam Honari",
    author_email="maryam.honari@eedi.co.uk",
    description="Eedi data loader",
    packages=find_packages("eedata"),
    package_dir={"": "eedata"},
    python_requires=">=3.8.0",
    include_package_data=True,
    install_requires=[
        "pandas>=2.0.0",
        "numpy>=1.20.0",
        'pyodbc>=4.0.34;platform_system=="Windows"',
        'pymssql>=2.2.7;platform_system!="Windows"',
        "gspread>=5.4.0",
        "azure-core>=1.26.4",
        "azure-data-tables>=12.4.2",
        "azure-storage-blob>=12.16.0",
        "python-dotenv>=1.0.0",
        "setuptools>=65.0.1",
        "ipython>=8.13.0",
    ],
    # url="https://github.com/",
    entry_points={},
)
# extra: nose2, moto
