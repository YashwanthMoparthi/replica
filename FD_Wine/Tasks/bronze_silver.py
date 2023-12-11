"""
This script contains a single class that is responsible for creating the bronze
and silver layers for the WINE feature domain.

This script contains the following
class:
    * CreateBronzeSilver - creates processes for Bronze and Silver SQL Tasks
"""


class CreateBronzeSilver:
    """
    A class used to create the bronze and silver layers for
    the WINE feature domain.

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
    create_stage_bronze_raw(schedule, pipe_name=FD_WINE_DATA_PIPE)
        Sends raw data into the silver clean table
    """

    def __init__(self, session, database, schema, warehouse):
        self.session = session
        self.database = database
        self.schema = schema
        self.warehouse = warehouse

    def create_stage_bronze_raw(self, schedule, pipe_name="FD_WINE_DATA_PIPE"):
        """
        Method creates a task that calls the FD_WINE_DATA_PIPE
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
                    CREATE OR REPLACE TASK {self.database}.{self.schema}.LOAD_FD_WINE_STAGE_INTO_BRONZE_RAW
                        warehouse={self.warehouse}
                        schedule='{schedule}'
                    AS 
                    ALTER PIPE {pipe_name} REFRESH;
                """
        ).collect()

    def create_bronze_raw_silver_clean(
        self, bronze_table, silver_table, after="LOAD_FD_WINE_STAGE_INTO_BRONZE_RAW"
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
                CREATE OR REPLACE TASK {self.database}.{self.schema}.LOAD_FD_WINE_BRONZE_RAW_INTO_SILVER_CLEAN
                    warehouse={self.warehouse}
                    after {self.database}.{self.schema}.{after}
                    as
                    MERGE INTO {self.database}.{self.schema}.{silver_table} AS TARGET
                    USING (
                    SELECT * 
                        FROM {self.database}.{self.schema}.{bronze_table} 
                        WHERE 
                            FIXED_ACIDITY IS NOT NULL AND 
                            VOLATILE_ACIDITY IS NOT NULL AND
                            CITRIC_ACID IS NOT NULL AND
                            RESIDUAL_SUGAR IS NOT NULL AND
                            CHLORIDES IS NOT NULL AND
                            FREE_SULFER_DIOXIDE IS NOT NULL AND
                            TOTAL_SULFER_DIOXIDE IS NOT NULL AND
                            DENSITY IS NOT NULL AND
                            PH IS NOT NULL AND
                            SULPHATES IS NOT NULL AND
                            ALCOHOL IS NOT NULL
                    ) AS SOURCE
                    ON TARGET.ID = SOURCE.ID
                    WHEN MATCHED THEN UPDATE SET
                        TARGET.ID = SOURCE.ID,
                        TARGET.FIXED_ACIDITY = SOURCE.FIXED_ACIDITY,
                        TARGET.VOLATILE_ACIDITY = SOURCE.VOLATILE_ACIDITY,
                        TARGET.CITRIC_ACID = SOURCE.CITRIC_ACID,
                        TARGET.RESIDUAL_SUGAR = SOURCE.RESIDUAL_SUGAR,
                        TARGET.CHLORIDES = SOURCE.CHLORIDES,
                        TARGET.FREE_SULFER_DIOXIDE = SOURCE.FREE_SULFER_DIOXIDE,
                        TARGET.TOTAL_SULFER_DIOXIDE = SOURCE.TOTAL_SULFER_DIOXIDE,
                        TARGET.DENSITY = SOURCE.DENSITY,
                        TARGET.PH = SOURCE.PH,
                        TARGET.SULPHATES = SOURCE.SULPHATES,
                        TARGET.ALCOHOL = SOURCE.ALCOHOL,
                        TARGET.QUALITY = SOURCE.QUALITY
                    WHEN NOT MATCHED THEN
                        INSERT(ID, FIXED_ACIDITY, VOLATILE_ACIDITY, CITRIC_ACID, RESIDUAL_SUGAR, CHLORIDES, FREE_SULFER_DIOXIDE, TOTAL_SULFER_DIOXIDE, DENSITY, PH, SULPHATES, ALCOHOL, QUALITY)
                        VALUES(SOURCE.ID, SOURCE.FIXED_ACIDITY, SOURCE.VOLATILE_ACIDITY, SOURCE.CITRIC_ACID, SOURCE.RESIDUAL_SUGAR, SOURCE.CHLORIDES, SOURCE.FREE_SULFER_DIOXIDE, SOURCE.TOTAL_SULFER_DIOXIDE, SOURCE.DENSITY, SOURCE.PH, SOURCE.SULPHATES, SOURCE.ALCOHOL, SOURCE.QUALITY);
            """
        ).collect()
