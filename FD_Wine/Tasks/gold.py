"""
This script contains a single class that creates the gold layer
for the WINE feature domain.

This script contains the following
classes:
    * CreateGold - creates processes for Gold SQL tasks
"""


# Functions
class CreateGold:
    """
    A class used to create the gold layer for the WINE feature domain.

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
    create_acid(gold_table_name, silver_table_name, after=LOAD_FD_WINE_BRONZE_RAW_INTO_SILVER_CLEAN)
        Preprocesses silver data into the Gold acid table.
    create_dioxide(gold_table_name, silver_table_name, after=LOAD_FD_WINE_BRONZE_RAW_INTO_SILVER_CLEAN)
        Preprocesses silver data into the Gold dioxide table.
    create_other_features(gold_table_name, silver_table_name, after=LOAD_FD_WINE_BRONZE_RAW_INTO_SILVER_CLEAN)
        Preprocesses silver data into the Gold other table.
    """

    def __init__(self, session, database, schema, warehouse):
        self.session = session
        self.database = database
        self.schema = schema
        self.warehouse = warehouse

    def create_acid(
        self,
        gold_table_name,
        silver_table_name,
        after="LOAD_FD_WINE_BRONZE_RAW_INTO_SILVER_CLEAN",
    ):
        """
        Method creates the acid feature table from the
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
                CREATE OR REPLACE TASK {self.database}.{self.schema}.LOAD_FD_WINE_SILVER_CLEAN_INTO_GOLD_ACID
                    warehouse={self.warehouse}
                    AFTER {self.database}.{self.schema}.{after}
                AS
                    MERGE INTO {self.database}.{self.schema}.{gold_table_name} AS TARGET
                    USING (
                        SELECT 
                            ID,
                            FIXED_ACIDITY,
                            VOLATILE_ACIDITY,
                            (FIXED_ACIDITY + VOLATILE_ACIDITY)/2 AS AVG_ACIDITY,
                            FIXED_ACIDITY + VOLATILE_ACIDITY AS SUM_ACIDITY
                            FROM {self.database}.{self.schema}.{silver_table_name}
                    ) AS SOURCE
                    ON TARGET.ID = SOURCE.ID
                    WHEN MATCHED THEN UPDATE SET
                        TARGET.ID = SOURCE.ID,
                        TARGET.FIXED_ACIDITY = SOURCE.FIXED_ACIDITY,
                        TARGET.VOLATILE_ACIDITY = SOURCE.VOLATILE_ACIDITY,
                        TARGET.AVG_ACIDITY = SOURCE.AVG_ACIDITY,
                        TARGET.SUM_ACIDITY = SOURCE.SUM_ACIDITY
                    WHEN NOT MATCHED THEN
                        INSERT(ID, FIXED_ACIDITY, VOLATILE_ACIDITY, AVG_ACIDITY, SUM_ACIDITY)
                        VALUES(SOURCE.ID, SOURCE.FIXED_ACIDITY, SOURCE.VOLATILE_ACIDITY, SOURCE.AVG_ACIDITY, SOURCE.SUM_ACIDITY);
            """
        ).collect()

    def create_dioxide(
        self,
        gold_table_name,
        silver_table_name,
        after="LOAD_FD_WINE_BRONZE_RAW_INTO_SILVER_CLEAN",
    ):
        """
        Method creates the dioxide feature table from the
        silver table.

        Parameters
        ----------
        gold_table_name : str
            gold table name
        silver_table_name : str
            silver table name
        after : str
            snowflake task name
        """

        _ = self.session.sql(
            f"""
                CREATE OR REPLACE TASK {self.database}.{self.schema}.LOAD_FD_WINE_SILVER_CLEAN_INTO_GOLD_DIOXIDE
                    warehouse={self.warehouse}
                    AFTER {self.database}.{self.schema}.{after}
                AS
                    MERGE INTO {self.database}.{self.schema}.{gold_table_name} AS TARGET
                    USING (
                        SELECT 
                            ID,
                            FREE_SULFER_DIOXIDE,
                            TOTAL_SULFER_DIOXIDE,
                            (FREE_SULFER_DIOXIDE + TOTAL_SULFER_DIOXIDE)/2 AS AVG_DIOXIDE,
                            FREE_SULFER_DIOXIDE + TOTAL_SULFER_DIOXIDE AS SUM_DIOXIDE
                            FROM {self.database}.{self.schema}.{silver_table_name}
                    ) AS SOURCE
                    ON TARGET.ID = SOURCE.ID
                    WHEN MATCHED THEN UPDATE SET
                        TARGET.ID = SOURCE.ID,
                        TARGET.FREE_SULFER_DIOXIDE = SOURCE.FREE_SULFER_DIOXIDE,
                        TARGET.TOTAL_SULFER_DIOXIDE = SOURCE.TOTAL_SULFER_DIOXIDE,
                        TARGET.AVG_DIOXIDE = SOURCE.AVG_DIOXIDE,
                        TARGET.SUM_DIOXIDE = SOURCE.SUM_DIOXIDE
                    WHEN NOT MATCHED THEN
                        INSERT(ID, FREE_SULFER_DIOXIDE, TOTAL_SULFER_DIOXIDE, AVG_DIOXIDE, SUM_DIOXIDE)
                        VALUES(SOURCE.ID, SOURCE.FREE_SULFER_DIOXIDE, SOURCE.TOTAL_SULFER_DIOXIDE, SOURCE.AVG_DIOXIDE, SOURCE.SUM_DIOXIDE);
            """
        ).collect()

    def create_other_features(
        self,
        gold_table_name,
        silver_table_name,
        after="LOAD_FD_WINE_BRONZE_RAW_INTO_SILVER_CLEAN",
    ):
        """
        Method creates other feature tables for the feature
        domain from the silver table.

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
                CREATE OR REPLACE TASK {self.database}.{self.schema}.LOAD_FD_WINE_SILVER_CLEAN_INTO_GOLD_OTHER
                    warehouse={self.warehouse}
                    AFTER {self.database}.{self.schema}.{after}
                AS
                    MERGE INTO {self.database}.{self.schema}.{gold_table_name} AS TARGET
                    USING (
                        SELECT 
                            ID,
                            CITRIC_ACID,
                            RESIDUAL_SUGAR,
                            CHLORIDES,
                            DENSITY,
                            PH,
                            SULPHATES,
                            ALCOHOL
                            FROM {self.database}.{self.schema}.{silver_table_name}
                    ) AS SOURCE
                    ON TARGET.ID = SOURCE.ID
                    WHEN MATCHED THEN UPDATE SET
                        TARGET.ID = SOURCE.ID,
                        TARGET.CITRIC_ACID = SOURCE.CITRIC_ACID,
                        TARGET.RESIDUAL_SUGAR = SOURCE.RESIDUAL_SUGAR,
                        TARGET.CHLORIDES = SOURCE.CHLORIDES,
                        TARGET.DENSITY = SOURCE.DENSITY,
                        TARGET.PH = SOURCE.PH,
                        TARGET.SULPHATES = SOURCE.SULPHATES,
                        TARGET.ALCOHOL = SOURCE.ALCOHOL
                    WHEN NOT MATCHED THEN
                        INSERT(ID, CITRIC_ACID, RESIDUAL_SUGAR, CHLORIDES, DENSITY, PH, SULPHATES, ALCOHOL)
                        VALUES(SOURCE.ID, SOURCE.CITRIC_ACID, SOURCE.RESIDUAL_SUGAR, SOURCE.CHLORIDES, SOURCE.DENSITY, SOURCE.PH, SOURCE.SULPHATES, SOURCE.ALCOHOL);
            """
        ).collect()
