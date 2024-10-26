from typing import Any
import os

import dlt
from dlt.common import pendulum
from dlt.sources.credentials import ConnectionStringCredentials

from dlt.sources.sql_database import sql_database, sql_table, Table

from sqlalchemy.sql.sqltypes import TypeEngine
import sqlalchemy as sa

from settings import (resource_list_1)

# Set the environment variable
os.environ["SCHEMA__NAMING"] = "direct"

def load_entire_database() -> None:
    """Use the sql_database source to completely load all tables in a database"""
    pipeline = dlt.pipeline(pipeline_name="rlf_pipeline_address_log", 
                            destination='bigquery', 
                            dataset_name="rlf_data_tables", 
                            progress="log",
                            dev_mode=False,
                            )

    # By default the sql_database source reflects all tables in the schema
    # The database credentials are sourced from the `.dlt/secrets.toml` configuration
    source = sql_database(backend="pyarrow", chunk_size=100000).parallelize().with_resources("Address_Log")

    source.Address_Log.apply_hints(
    incremental=dlt.sources.incremental("Log_Updated_At", initial_value=pendulum.DateTime(2022, 1, 1, 0, 0, 0)),
    primary_key="LogID"
)

    # Run the pipeline. For a large db this may take a while
    info = pipeline.run(source, write_disposition="merge")
    print(info)

if __name__ == "__main__":
    load_entire_database()

