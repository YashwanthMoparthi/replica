"""
This script contains a single class that is responsible for creating the bronze
and silver layers for the SALES feature domain.

This script contains the following
class:
    * CreateBronzeSilver - creates processes for Bronze and Silver SQL Tasks
"""


class CreateBronzeSilver:
    """
    A class used to create the bronze and silver layers for
    the SALES feature domain.

    Attributes
    ----------
    session : str
        snowflake session
    database : str
        database name
    schema : str
        schema name
    warehouse : str
        warhouse name

    Methods
    -------
    create_stage_bronze_raw(schedule, pipe_name=FD_SALES_DATA_PIPE)
        Sends raw data into the silver clean table
    """

    def __init__(self, session, database, schema, warehouse):
        self.session = session
        self.database = database
        self.schema = schema
        self.warehouse = warehouse

    def create_stage_bronze_raw(self, schedule, pipe_name="FD_SALES_DATA_PIPE"):
        """
        Method creates a task that calls the FD_SALES_DATA_PIPE
        in order to send the raw data into the silver clean table.

        Parameters
        ----------
        schedule : str
            scheduler time
        pipe_name : str
            snowflake pipe name
        """

        _ = self.session.sql(
            f"""
                CREATE OR REPLACE TASK {self.database}.{self.schema}.LOAD_FD_SALES_STAGE_INTO_BRONZE_RAW
                    warehouse={self.warehouse}
                    schedule='{schedule}'
                AS 
                ALTER PIPE {pipe_name} REFRESH;
            """
        ).collect()

    def create_bronze_raw_silver_clean(
        self, bronze_table, silver_table, after="LOAD_FD_SALES_STAGE_INTO_BRONZE_RAW"
    ):
        """
        Method creates a task to upsert the raw data into the silver clean data.

        Parameters
        ----------
        bronze_table : str
            bronze table name
        silver_table : str
            silver table name
        after : str
            snowflake task name
        """

        _ = self.session.sql(
            f"""
                CREATE OR REPLACE TASK {self.database}.{self.schema}.LOAD_FD_SALES_BRONZE_RAW_INTO_SILVER_CLEAN
                    warehouse={self.warehouse}
                    after {self.database}.{self.schema}.{after}
                    as MERGE INTO {self.database}.{self.schema}.{silver_table} AS TARGET
                USING (
                    SELECT * 
                        FROM {self.database}.{self.schema}.{bronze_table} 
                        WHERE 
                            transaction_id IS NOT NULL AND 
                            customer_id IS NOT NULL AND
                            transaction_date IS NOT NULL AND
                            product_id IS NOT NULL AND
                            quantity IS NOT NULL AND
                            price IS NOT NULL AND
                            total_amount IS NOT NULL
                ) AS SOURCE
                ON TARGET.transaction_id = SOURCE.transaction_id
                WHEN MATCHED THEN UPDATE SET
                    TARGET.customer_id = SOURCE.customer_id,
                    TARGET.transaction_date = SOURCE.transaction_date,
                    TARGET.product_id = SOURCE.product_id,
                    TARGET.quantity = SOURCE.quantity,
                    TARGET.price = SOURCE.price,
                    TARGET.total_amount = SOURCE.total_amount
                WHEN NOT MATCHED THEN
                    INSERT(transaction_id, customer_id, transaction_date, product_id, quantity, price, total_amount)
                    VALUES(SOURCE.transaction_id, SOURCE.customer_id, SOURCE.transaction_date, SOURCE.product_id, SOURCE.quantity, SOURCE.price, SOURCE.total_amount);
            """
        ).collect()
