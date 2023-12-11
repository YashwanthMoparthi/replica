"""
This script contains a single class that creates the gold layer
for the SALES feature domain.

This script contains the following
classes:
    * CreateGold - creates processes for Gold SQL tasks
"""


# Functions
class CreateGold:
    """
    A class used to create the gold layer for the SALES feature domain.

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
    create_gold(gold_table_name, silver_table_name, after=LOAD_FD_SALES_BRONZE_RAW_INTO_SILVER_CLEAN)
        Preprocesses silver data into the Gold SALES table.
    """

    def __init__(self, session, database, schema, warehouse):
        self.session = session
        self.database = database
        self.schema = schema
        self.warehouse = warehouse

    def create_gold(
        self,
        gold_table_name,
        silver_table_name,
        after="LOAD_FD_SALES_BRONZE_RAW_INTO_SILVER_CLEAN",
    ):
        """
        Method creates the GOLD feature table from the
        silver table.

        Parameters
        ------------
        gold_table_name : str
            gold table name
        silver_table_name : str
            silver table name
        after : str
            snowflake task name
        """

        _ = self.session.sql(
            f"""
                CREATE OR REPLACE TASK {self.database}.{self.schema}.LOAD_FD_SALES_SILVER_CLEAN_INTO_GOLD
                    warehouse={self.warehouse}
                    AFTER {after}
                AS
                    MERGE INTO {self.database}.{self.schema}.{gold_table_name} AS TARGET
                    USING (
                        SELECT * FROM {self.database}.{self.schema}.{silver_table_name}
                    ) AS SOURCE
                    ON TARGET.transaction_id = SOURCE.transaction_id
                    WHEN MATCHED THEN UPDATE SET
                        TARGET.customer_id = SOURCE.customer_id,
                        TARGET.transaction_date = SOURCE.transaction_date,
                        TARGET.product_id = SOURCE.product_id,
                        TARGET.quantity = SOURCE.quantity,
                        TARGET.price = SOURCE.price,
                        TARGET.total_amount = SOURCE.total_amount,
                        TARGET.discounted_amount = CASE
                            WHEN SOURCE.total_amount > 50.00 THEN SOURCE.total_amount * 0.9
                            ELSE SOURCE.total_amount
                        END,
                        TARGET.is_high_value_customer = CASE
                            WHEN SOURCE.total_amount > 50.00 THEN true
                            ELSE false
                        END
                    WHEN NOT MATCHED THEN
                        INSERT(transaction_id, customer_id, transaction_date, product_id, quantity, price, total_amount, discounted_amount, is_high_value_customer)
                        VALUES(SOURCE.transaction_id, SOURCE.customer_id, SOURCE.transaction_date, SOURCE.product_id, SOURCE.quantity, SOURCE.price, SOURCE.total_amount,
                               CASE WHEN SOURCE.total_amount > 50.00 THEN SOURCE.total_amount * 0.9 ELSE SOURCE.total_amount END,
                               CASE WHEN SOURCE.total_amount > 50.00 THEN true ELSE false END);
            """
        ).collect()
