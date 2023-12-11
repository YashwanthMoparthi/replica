USE DATABASE FEATURESTORE_DB;
USE SCHEMA FEATURESTORE_SCHEMA;
USE ROLE FEATURESTORE_ROLE;

CREATE OR REPLACE PROCEDURE SP_FD_SALES_TABLES(layer VARCHAR)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS'
BEGIN
    IF (UPPER(layer) IN (''BRONZE'', ''ALL'')) THEN
		CREATE OR REPLACE TABLE BRONZE_FD_SALES_RAW (
		transaction_id INT PRIMARY KEY,
		customer_id INT,
		transaction_date DATE,
		product_id INT,
		quantity INT,
		price DECIMAL(10, 2),
		total_amount DECIMAL(10, 2)
	);
    END IF;

    IF (UPPER(layer) IN (''SILVER'', ''ALL'')) THEN
		CREATE OR REPLACE TABLE SILVER_FD_SALES_CLEAN (
		transaction_id INT PRIMARY KEY,
		customer_id INT,
		transaction_date DATE,
		product_id INT,
		quantity INT,
		price DECIMAL(10, 2),
		total_amount DECIMAL(10, 2)
	);
    END IF;

    IF (UPPER(layer) IN (''GOLD'', ''ALL'')) THEN
		CREATE OR REPLACE TABLE GOLD_FD_SALES(
		transaction_id INT PRIMARY KEY,
		customer_id INT,
		transaction_date DATE,
		product_id INT,
		quantity INT,
		price DECIMAL(10, 2),
		total_amount DECIMAL(10, 2),
		discounted_amount DECIMAL(10, 2),
		is_high_value_customer BOOLEAN
		);
    END IF;
	
    RETURN CONCAT(''Created Tables for '', LAYER, '' LAYER(s)'');
END';