"""
This script is the main entry point when snowflake calls the
python based store procedure. 

The script requires all processes and codes to create the bronze, silver, 
and gold layers for the feature domain SALES.

This script contains the following
functions:
    * alter_tasks - starts snowflake SQL tasks
    * main - the main function of the script
"""


from snowflake.snowpark import Session
from bronze_silver import CreateBronzeSilver
from gold import CreateGold


def alter_tasks(session, database, schema):
    """
    Method starts the snowflake tasks for the feature domain.

    Parameters
    ------------
    session : str
       snowflake session
    database : str
        database name
    schema : string
        schema name
    """

    task_lst = [
        "LOAD_FD_SALES_STAGE_INTO_BRONZE_RAW",
        "LOAD_FD_SALES_BRONZE_RAW_INTO_SILVER_CLEAN",
        "LOAD_FD_SALES_SILVER_CLEAN_INTO_GOLD",
    ]
    for task in list(reversed(task_lst)):
        _ = session.sql(f"ALTER TASK {database}.{schema}.{task} RESUME").collect()


# def main(session: Session, database, schema, warehouse):
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
    schedule = "3 MINUTES"
    bronze_table = "BRONZE_FD_SALES_RAW"
    silver_table = "SILVER_FD_SALES_CLEAN"
    gold_table = "GOLD_FD_SALES"

    # Creating bronze task
    bronze_silver = CreateBronzeSilver(session, database, schema, warehouse)
    bronze_silver.create_stage_bronze_raw(schedule)

    # Creating silver task
    bronze_silver.create_bronze_raw_silver_clean(bronze_table, silver_table)

    # Creating gold task
    gold = CreateGold(session, database, schema, warehouse)
    gold.create_gold(gold_table, silver_table)

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
