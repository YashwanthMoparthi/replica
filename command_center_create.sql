USE DATABASE FEATURESTORE_DB;
USE SCHEMA FEATURESTORE_SCHEMA;
USE ROLE FEATURESTORE_ROLE;

CREATE OR REPLACE PROCEDURE COMMAND_CENTER_CREATE("FEATURE_DOMAIN" VARCHAR(16777216), "OBJECTIVE" VARCHAR(16777216), "LAYER" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
AS 'DECLARE 
    QUERY VARCHAR; 
    EXECUTE_QUERY VARCHAR;
BEGIN
    IF (UPPER(OBJECTIVE) NOT IN (''TABLES'', ''STAGES'', ''PIPES'', ''STAGES & PIPES'')) THEN
        RETURN ''OBJECTIVE has to be set to: Tables, Pipes, Stages or Stages & Pipes'';
        
    ELSEIF ((UPPER(OBJECTIVE) = ''TABLES'' AND UPPER(layer) NOT IN (''BRONZE'', ''SILVER'', ''GOLD'', ''ALL'')) OR (UPPER(OBJECTIVE) = ''TABLES'' AND layer IS NULL)) THEN
        RETURN ''Layer has to be set to bronze, silver, or gold'';
    
    ELSEIF (UPPER(OBJECTIVE) = ''TABLES'' AND UPPER(layer) IN (''BRONZE'', ''SILVER'', ''GOLD'', ''ALL'')) THEN
        QUERY := CONCAT(''SP'', ''_'', FEATURE_DOMAIN, ''_'', OBJECTIVE);
        EXECUTE_QUERY := CONCAT(''CALL '', QUERY, ''(\\'''', layer,''\\'''', '')'');
        EXECUTE IMMEDIATE EXECUTE_QUERY;
        RETURN CONCAT(''Executed store procedure '', EXECUTE_QUERY);

    ELSEIF (UPPER(OBJECTIVE) IN (''PIPES'', ''STAGES'', ''STAGES & PIPES'') AND layer IS NULL) THEN
        QUERY := CONCAT(''SP'', ''_'', FEATURE_DOMAIN, ''_'', ''PIPELINE_STAGE'');
        EXECUTE_QUERY := CONCAT(''CALL '', QUERY, ''(\\'''', OBJECTIVE,''\\'''', '')'');
        EXECUTE IMMEDIATE EXECUTE_QUERY;
        RETURN CONCAT(''Executed store procedure '', EXECUTE_QUERY);

    END IF;
END';
