"""
This script is the main entry point when snowflake calls the
python based store procedure. 

The script requires all codes to create the bronze,, silver, 
and gold layers for the feature domain PARAMETER.

This script contains the following
functions:
    * alter_tasks - starts snowflake SQL tasks
    * main - the main function of the script
"""


from snowflake.snowpark import Session
from gold import CreateGold


def alter_tasks(session, database, schema, main="LOAD_INIT", SUSPEND=False):
    """
    Method suspends or starts the snowflake tasks for the feature domain.

    Parameters
    ------------
    session : str
       snowflake session
    database : str
        database name
    schema : str
        schema name
    """
    if SUSPEND:
        _ = session.sql(f"ALTER TASK {database}.{schema}.{main} SUSPEND;").collect()

    if not SUSPEND:
        task_lst = [
            main,
            "LOAD_FD_PARAMETER_SILVER_CLEAN_INTO_GOLD_ACID",
            "LOAD_FD_PARAMETER_SILVER_CLEAN_INTO_GOLD_DIOXIDE",
            "LOAD_FD_PARAMETER_SILVER_CLEAN_INTO_GOLD_OTHER",
        ]
        for task in list(reversed(task_lst)):
            _ = session.sql(f"ALTER TASK {database}.{schema}.{task} RESUME;").collect()


def main(session: Session) -> str:
    """
    Method is the main handler for intiating the store
    procedure that creates the tasks for the feature
    domain.

    Parameters
    ------------
    session : str
       snowflake session
    """

    database = "FEATURESTORE_DB"
    schema = "FEATURESTORE_SCHEMA"
    warehouse = "FEATURESTORE_WH"
    silver_table = "SILVER_FD_PARAMETER_CLEAN"
    acid_table = "GOLD_FD_PARAMETER_ACIDITY"
    dioxide_table = "GOLD_FD_PARAMETER_DIOXIDE"
    other_table = "GOLD_FD_PARAMETER_OTHER"

    # Supending init node
    alter_tasks(session, database, schema, SUSPEND=True)
    
    # Creating gold task
    gold = CreateGold(session, database, schema, warehouse)
    gold.create_acid(acid_table, silver_table)
    gold.create_dioxide(dioxide_table, silver_table)
    gold.create_other_features(other_table, silver_table)

    # Starting tasks
    alter_tasks(session, database, schema)

    return "Successfully created and started tasks"


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
