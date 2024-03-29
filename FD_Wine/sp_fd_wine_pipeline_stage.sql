USE DATABASE FEATURESTORE_DB;
USE SCHEMA FEATURESTORE_SCHEMA;
USE ROLE FEATURESTORE_ROLE;


CREATE OR REPLACE PROCEDURE SP_FD_WINE_PIPELINE_STAGE("OBJECTIVE" VARCHAR)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS ' 
BEGIN
    IF (UPPER(OBJECTIVE) IN (''STAGES'', ''STAGES & PIPES'')) THEN
        CREATE OR REPLACE STAGE FD_WINE_DATA_STAGE 
	        DIRECTORY = ( ENABLE = true );
    END IF;
    IF (UPPER(OBJECTIVE) IN (''PIPES'', ''STAGES & PIPES'')) THEN
        CREATE OR REPLACE PIPE FD_WINE_DATA_PIPE
            auto_ingest=false 
        AS
        COPY INTO BRONZE_FD_WINE_RAW FROM @FD_WINE_DATA_STAGE FILE_FORMAT=(FORMAT_NAME=''CSV_FORMAT'');
    END IF;
END';
