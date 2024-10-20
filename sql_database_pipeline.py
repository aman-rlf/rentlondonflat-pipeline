# flake8: noqa
import humanize
from typing import Any
import os

import dlt
from dlt.common import pendulum
from dlt.sources.credentials import ConnectionStringCredentials

from dlt.sources.sql_database import sql_database, sql_table, Table

from sqlalchemy.sql.sqltypes import TypeEngine
import sqlalchemy as sa

from settings import (resource_list_1)

def load_select_tables_from_database() -> None:
    """Use the sql_database source to reflect an entire database schema and load select tables from it.

    This example sources data from the public Rfam MySQL database.
    """
    # Create a pipeline
    pipeline = dlt.pipeline(pipeline_name="rfam", destination='duckdb', dataset_name="rfam_data")

    # Credentials for the sample database.
    # Note: It is recommended to configure credentials in `.dlt/secrets.toml` under `sources.sql_database.credentials`
    credentials = ConnectionStringCredentials(
        "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam"
    )
    # To pass the credentials from `secrets.toml`, comment out the above credentials.
    # And the credentials will be automatically read from `secrets.toml`.

    # Configure the source to load a few select tables incrementally
    source_1 = sql_database(credentials).with_resources("family", "clan")

    # Add incremental config to the resources. "updated" is a timestamp column in these tables that gets used as a cursor
    source_1.family.apply_hints(incremental=dlt.sources.incremental("updated"))
    source_1.clan.apply_hints(incremental=dlt.sources.incremental("updated"))

    # Run the pipeline. The merge write disposition merges existing rows in the destination by primary key
    info = pipeline.run(source_1, write_disposition="merge")
    print(info)

    # Load some other tables with replace write disposition. This overwrites the existing tables in destination
    source_2 = sql_database(credentials).with_resources("features", "author")
    info = pipeline.run(source_2, write_disposition="replace")
    print(info)

    # Load a table incrementally with append write disposition
    # this is good when a table only has new rows inserted, but not updated
    source_3 = sql_database(credentials).with_resources("genome")
    source_3.genome.apply_hints(incremental=dlt.sources.incremental("created"))

    info = pipeline.run(source_3, write_disposition="append")
    print(info)


def load_entire_database() -> None:
    """Use the sql_database source to completely load all tables in a database"""
    pipeline = dlt.pipeline(pipeline_name="rlf_pipeline", 
                            destination='bigquery', 
                            dataset_name="rlf_data_tables", 
                            progress="log",
                            dev_mode=False)

    # By default the sql_database source reflects all tables in the schema
    # The database credentials are sourced from the `.dlt/secrets.toml` configuration
    source = sql_database(backend="pyarrow", chunk_size=100000, naming="direct").parallelize().with_resources(*resource_list_1)

    # Run the pipeline. For a large db this may take a while
    info = pipeline.run(source, write_disposition="replace")
    print(humanize.precisedelta(pipeline.last_trace.finished_at - pipeline.last_trace.started_at))
    print(info)






if __name__ == "__main__":
    # Load selected tables with different settings
    # load_select_tables_from_database()

    # load a table and select columns
    # select_columns()

    load_entire_database()
    # select_with_end_value_and_row_order()

    # Load tables with the standalone table resource
    #load_standalone_table_resource()

    # Load all tables from the database.
    # Warning: The sample database is very large
    # load_entire_database()
