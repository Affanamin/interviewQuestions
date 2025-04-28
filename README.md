# interviewQuestions
All interview questions related to SPARK, SQL in Data enginnering 


For Synapse:::


### Azure Synapse::::::::::

We have data in our silver layer in ADLS, we created managed identity to create a connectivity link between synapse and ADLS (external) then we created schema and in that schema we created views using openrowset, then we need to create external tables so that our transformed data should be inserted in gold container in ADLS as well so for creating external table we have used CETAS, but we need 3 more things:
1. -- Credentials
2. -- External Data SOurce
3. -- External FIle formats 

so for creds:

1.
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'password';

CREATE DATABASE SCOPED CREDENTIAL credential_aff
WITH IDENTITY = 'Managed Identity'
--------------------------------------------------------------------------------------
2. 
CREATE EXTERNAL DATA SOURCE source_gold
WITH 
(
    LOCATION = 'https://adventureprojstorageacc.dfs.core.windows.net/gold',
    CREDENTIAL = credential_aff
) 
--------------------------------------------------------------------------------------
3.
CREATE EXTERNAL FILE FORMAT file_format_name
WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec' 
    );

 Now using CETAS we can create external table:

 CREATE EXTERNAL TABLE gold.extsales WITH (
    LOCATION = 'extsales',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = file_format_name
) AS SELECT * from gold.sales;


For views using openrowset, we have created like:

CREATE VIEW gold.sales
AS 
SELECT * FROM OPENROWSET(
    BULK 'https://adventureprojstorageacc.dfs.core.windows.net/silver/AdventureWorks_Sales/',
    FORMAT = 'PARQUET'
) as query1
