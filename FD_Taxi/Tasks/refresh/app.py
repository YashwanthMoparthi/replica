"""
This script is the main entry point when snowflake calls the
python based store procedure. 

The script will create a SQL tasks that calls the python based
store procedure.

This script contains the following
functions:
    * alter_tasks - starts snowflake SQL Tasks
    * main - the main function of the script
"""

from snowflake.snowpark import Session


def alter_tasks(session, database, schema):
    """
    Method starts SQL task within the snowflake environment.

    Parameters
    ------------
    session : string
        Snowflake session
    database : string
        database name
    schema : string
        schema name
    """

    _ = session.sql(
        f"ALTER TASK {database}.{schema}.LOAD_FD_TAXI_FEATURES RESUME"
    ).collect()


def main(session: Session) -> str:
    """
    Method is main handler when executing store procedure. It calls the
    procedure which intitiates the data flow process from raw data to
    gold data.

    Parameters
    ------------
    session : string
        Snowflake session

    Return
    ------------
    string printed to snowflake environment
    """

    database = "FEATURESTORE_DB"
    schema = "FEATURESTORE_SCHEMA"
    warehouse = "FEATURESTORE_WH"
    schedule = "3 MINUTES"
    build_name = "SP_FD_TAXI_BUILD"

    # Creating tasks that calls build
    _ = session.sql(
        f"""
        CREATE OR REPLACE TASK {database}.{schema}.LOAD_FD_TAXI_FEATURES
            warehouse={warehouse}
            schedule='{schedule}'
        AS 
        CALL {database}.{schema}.{build_name}();
    """
    ).collect()

    # Starting task
    alter_tasks(session, database, schema)

    return "Successfully created Tasks"


if __name__ == "__main__":
    import os
    import sys

    current_dir = os.getcwd()
    sys.path.append(current_dir)

    from utils import snowpark_utils

    session = snowpark_utils.get_snowpark_session()

    if len(sys.argv) > 1:
        print(main(session, *sys.argv[1:]))  # type: ignore
    else:
        print(main(session))  # type: ignore

    session.close()
