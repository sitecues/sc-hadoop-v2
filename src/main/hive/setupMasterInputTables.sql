-- This works of the raw (as opposed to clean) input table so that we can cross-check with the output of
-- setupNomInputTables.sql which works off of clean data

-- -----------------------------------------------------------------------------
-- SET UP THE MASTER TABLE
DROP TABLE IF EXISTS raw_master_log;
CREATE EXTERNAL TABLE raw_master_log (line string) location 's3://logs.sitecues.com/raw/wsprd/V2/wsprd3.sitecues.com/metrics/';
-- -----------------------------------------------------------------------------

-- -----------------------------------------------------------------------------
-- SET UP THE metadata table for the data in the raw clean nom logs.
DROP TABLE IF EXISTS raw_master_log_meta;
CREATE TABLE raw_master_log_meta STORED AS SEQUENCEFILE AS 
SELECT 
 tbl.sourceFileAbsolutePath,
 tbl.fileName,
 COUNT(*) AS lineCount 
FROM (
 SELECT
  INPUT__FILE__NAME AS sourceFileAbsolutePath,
  SPLIT(INPUT__FILE__NAME,'metrics/')[1] AS fileName
 FROM raw_master_log
) AS tbl GROUP BY  tbl.sourceFileAbsolutePath, tbl.fileName ORDER BY tbl.fileName ASC ;
-- -----------------------------------------------------------------------------

-- -----------------------------------------------------------------------------
-- Set up an optomized  version of the clean_nom_log table
DROP TABLE IF EXISTS optimized_raw_master_log;
CREATE TABLE optimized_raw_master_log STORED AS SEQUENCEFILE AS 
SELECT 
 INPUT__FILE__NAME AS sourceFileAbsolutePath,
 SPLIT(INPUT__FILE__NAME,'metrics/')[1] AS fileName, 
 line 
FROM raw_master_log; 
-- -----------------------------------------------------------------------------

-- -----------------------------------------------------------------------------
-- Check consistency of raw_master_log And optimized_raw_master_log
DROP TABLE IF EXISTS is_optimized_raw_master_log_consistent;
CREATE TABLE is_optimized_raw_master_log_consistent STORED AS SEQUENCEFILE AS 
SELECT 
 sourceCount,
 optimizedCount,
 (sourceCount=optimizedCount) AS isConsistent
FROM 
(SELECT 1 AS theKey, COUNT(*) AS sourceCount FROM raw_master_log) AS tbl1
JOIN
(SELECT 1 AS theKey, COUNT(*) AS optimizedCount FROM optimized_raw_master_log) AS tbl2
ON tbl1.theKey = tbl2.theKey ;
-- -----------------------------------------------------------------------------

-- -----------------------------------------------------------------------------
-- Set up an event counts  from the optomized version of the clean_nom_log table
DROP TABLE IF EXISTS raw_master_log_event_counts;
CREATE TABLE raw_master_log_event_counts STORED AS SEQUENCEFILE AS
SELECT 
 fileName,
 name,
 COUNT(*) AS occuranceCount
FROM (
 SELECT 
  fileName, 
  get_json_object(line, '$.clientData.name') AS name
 FROM optimized_raw_master_log
) AS tbl group by fileName,name ORDER BY fileName ASC, occuranceCount DESC ;
-- -----------------------------------------------------------------------------

