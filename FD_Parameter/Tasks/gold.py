"""
This script contains a single class that creates the gold layer
for the PARAMETER feature domain.

This script contains the following
classes:
    * CreateGold - creates processes for Gold SQL tasks
"""


# Functions
class CreateGold:
    """
    A class used to create the gold layer for the PARAMETER feature domain.

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
    create_acid(gold_table_name, silver_table_name, after=LOAD_INIT)
        Preprocesses silver data into the Gold acid table.
    create_dioxide(gold_table_name, silver_table_name, after=LOAD_INIT)
        Preprocesses silver data into the Gold dioxide table.
    create_other_features(gold_table_name, silver_table_name, after=LOAD_INIT)
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
        after="LOAD_INIT",
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
                CREATE OR REPLACE TASK {self.database}.{self.schema}.LOAD_FD_PARAMETER_SILVER_CLEAN_INTO_GOLD_ACID
                    warehouse={self.warehouse}
                    AFTER {self.database}.{self.schema}.{after}
                AS
                    DECLARE date_val string;
                BEGIN
                    IF (system$get_predecessor_return_value() = 'ALL') THEN
                        date_val := (SELECT MIN(T_DATETIME) FROM SILVER_FD_PARAMETER_CLEAN);
                    ELSEIF (system$get_predecessor_return_value() != 'ALL') THEN
                        date_val := (SELECT MIN(T_DATETIME) FROM SILVER_FD_PARAMETER_CLEAN);
                    END IF;

                    MERGE INTO {self.database}.{self.schema}.{gold_table_name} AS TARGET
                    USING (
                        SELECT 
                            T_DATETIME,
                            FIXED_ACIDITY,
                            VOLATILE_ACIDITY,
                            (FIXED_ACIDITY + VOLATILE_ACIDITY)/2 AS AVG_ACIDITY,
                            FIXED_ACIDITY + VOLATILE_ACIDITY AS SUM_ACIDITY
                            FROM {self.database}.{self.schema}.{silver_table_name}
                            WHERE T_DATETIME>=:date_val
                    ) AS SOURCE
                    ON TARGET.T_DATETIME = SOURCE.T_DATETIME
                    WHEN MATCHED THEN UPDATE SET
                        TARGET.T_DATETIME = SOURCE.T_DATETIME,
                        TARGET.FIXED_ACIDITY = SOURCE.FIXED_ACIDITY,
                        TARGET.VOLATILE_ACIDITY = SOURCE.VOLATILE_ACIDITY,
                        TARGET.AVG_ACIDITY = SOURCE.AVG_ACIDITY,
                        TARGET.SUM_ACIDITY = SOURCE.SUM_ACIDITY
                    WHEN NOT MATCHED THEN
                        INSERT(T_DATETIME, FIXED_ACIDITY, VOLATILE_ACIDITY, AVG_ACIDITY, SUM_ACIDITY)
                        VALUES(SOURCE.T_DATETIME, SOURCE.FIXED_ACIDITY, SOURCE.VOLATILE_ACIDITY, SOURCE.AVG_ACIDITY, SOURCE.SUM_ACIDITY);
                END;
            """
        ).collect()

    def create_dioxide(
        self,
        gold_table_name,
        silver_table_name,
        after="LOAD_INIT",
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
                CREATE OR REPLACE TASK {self.database}.{self.schema}.LOAD_FD_PARAMETER_SILVER_CLEAN_INTO_GOLD_DIOXIDE
                    warehouse={self.warehouse}
                    AFTER {self.database}.{self.schema}.{after}
                AS
                    DECLARE date_val string;
                BEGIN
                    IF (system$get_predecessor_return_value() = 'ALL') THEN
                        date_val := (SELECT MIN(T_DATETIME) FROM SILVER_FD_PARAMETER_CLEAN);
                    ELSEIF (system$get_predecessor_return_value() != 'ALL') THEN
                        date_val := (SELECT MIN(T_DATETIME) FROM SILVER_FD_PARAMETER_CLEAN);
                    END IF;

                    MERGE INTO {self.database}.{self.schema}.{gold_table_name} AS TARGET
                    USING (
                        SELECT 
                            T_DATETIME,
                            FREE_SULFER_DIOXIDE,
                            TOTAL_SULFER_DIOXIDE,
                            (FREE_SULFER_DIOXIDE + TOTAL_SULFER_DIOXIDE)/2 AS AVG_DIOXIDE,
                            FREE_SULFER_DIOXIDE + TOTAL_SULFER_DIOXIDE AS SUM_DIOXIDE
                            FROM {self.database}.{self.schema}.{silver_table_name}
                            WHERE T_DATETIME>=:date_val
                    ) AS SOURCE
                    ON TARGET.T_DATETIME = SOURCE.T_DATETIME
                    WHEN MATCHED THEN UPDATE SET
                        TARGET.T_DATETIME = SOURCE.T_DATETIME,
                        TARGET.FREE_SULFER_DIOXIDE = SOURCE.FREE_SULFER_DIOXIDE,
                        TARGET.TOTAL_SULFER_DIOXIDE = SOURCE.TOTAL_SULFER_DIOXIDE,
                        TARGET.AVG_DIOXIDE = SOURCE.AVG_DIOXIDE,
                        TARGET.SUM_DIOXIDE = SOURCE.SUM_DIOXIDE
                    WHEN NOT MATCHED THEN
                        INSERT(T_DATETIME, FREE_SULFER_DIOXIDE, TOTAL_SULFER_DIOXIDE, AVG_DIOXIDE, SUM_DIOXIDE)
                        VALUES(SOURCE.T_DATETIME, SOURCE.FREE_SULFER_DIOXIDE, SOURCE.TOTAL_SULFER_DIOXIDE, SOURCE.AVG_DIOXIDE, SOURCE.SUM_DIOXIDE);
                END;
            """
        ).collect()

    def create_other_features(
        self,
        gold_table_name,
        silver_table_name,
        after="LOAD_INIT",
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
                CREATE OR REPLACE TASK {self.database}.{self.schema}.LOAD_FD_PARAMETER_SILVER_CLEAN_INTO_GOLD_OTHER
                    warehouse={self.warehouse}
                    AFTER {self.database}.{self.schema}.{after}
                AS
                    DECLARE date_val string;
                BEGIN
                    IF (system$get_predecessor_return_value() = 'ALL') THEN
                        date_val := (SELECT MIN(T_DATETIME) FROM SILVER_FD_PARAMETER_CLEAN);
                    ELSEIF (system$get_predecessor_return_value() != 'ALL') THEN
                        date_val := (SELECT MIN(T_DATETIME) FROM SILVER_FD_PARAMETER_CLEAN);
                    END IF;

                    MERGE INTO {self.database}.{self.schema}.{gold_table_name} AS TARGET
                    USING (
                        SELECT 
                            T_DATETIME,
                            CITRIC_ACID,
                            RESIDUAL_SUGAR,
                            CHLORIDES,
                            DENSITY,
                            PH,
                            SULPHATES,
                            ALCOHOL
                            FROM {self.database}.{self.schema}.{silver_table_name}
                            WHERE T_DATETIME>=:date_val
                    ) AS SOURCE
                    ON TARGET.T_DATETIME = SOURCE.T_DATETIME
                    WHEN MATCHED THEN UPDATE SET
                        TARGET.T_DATETIME = SOURCE.T_DATETIME,
                        TARGET.CITRIC_ACID = SOURCE.CITRIC_ACID,
                        TARGET.RESIDUAL_SUGAR = SOURCE.RESIDUAL_SUGAR,
                        TARGET.CHLORIDES = SOURCE.CHLORIDES,
                        TARGET.DENSITY = SOURCE.DENSITY,
                        TARGET.PH = SOURCE.PH,
                        TARGET.SULPHATES = SOURCE.SULPHATES,
                        TARGET.ALCOHOL = SOURCE.ALCOHOL
                    WHEN NOT MATCHED THEN
                        INSERT(T_DATETIME, CITRIC_ACID, RESIDUAL_SUGAR, CHLORIDES, DENSITY, PH, SULPHATES, ALCOHOL)
                        VALUES(SOURCE.T_DATETIME, SOURCE.CITRIC_ACID, SOURCE.RESIDUAL_SUGAR, SOURCE.CHLORIDES, SOURCE.DENSITY, SOURCE.PH, SOURCE.SULPHATES, SOURCE.ALCOHOL);
                END;
            """
        ).collect()
