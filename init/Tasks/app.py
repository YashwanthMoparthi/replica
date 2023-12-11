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

    # Creating tasks that calls build
    param_1 = session.sql(f"SELECT PARAM_1 FROM {database}.{schema}.PARAMETERS").collect()[0][0]

    _ = session.sql(
        f"""
        CALL SYSTEM$SET_RETURN_VALUE('{param_1}');
    """
    ).collect()

    # Starting task
    return "Successfully executed init check"


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
