-- -----------------------------------------------------------------------------
-- -----------------------------------------------------------------------------
-- Setup QA tables for nom stuff.
-- These tables will not be generated under dev runs. 
-- They can be built seperately.
-- TODO: Work out better naming and docs. Integrate variables, run on 
-- demand especially if the bootstrap hive system fails. when
-- -----------------------------------------------------------------------------

-- -----------------------------------------------------------------------------
-- -----------------------------------------------------------------------------
-- REQUIRED VARIABLES TO BE SET.
-- CNL_SOURCE_TBL_NAME AS the clean nom log raw source table name.
-- OCNL_SOURCE_TBL AS the Optomised Clean Nom Log source table name.
-- -----------------------------------------------------------------------------
-- -----------------------------------------------------------------------------

-- -----------------------------------------------------------------------------
-- NOM_CONSISTENTCY_TBL
-- -----------------------------------------------------------------------------
SET NOM_CONSISTENTCY_TBL_NAME=cln_consistent_log ;
-- Consistency check in event counts between clean_nom_log and optimized_clean_nom_log
-- * is_optimized_clean_nom_log_consistent *
DROP TABLE IF EXISTS ${hiveconf:NOM_CONSISTENTCY_TBL_NAME};
CREATE TABLE ${hiveconf:NOM_CONSISTENTCY_TBL_NAME} AS 
SELECT
'${hiveconf:CNL_SOURCE_TBL_NAME}' AS sourceTbl,
'${hiveconf:OCNL_SOURCE_TBL}' AS destTbl,
 sourceCount,
 optimizedCount,
 (sourceCount=optimizedCount) AS isConsistent
FROM 
(SELECT 1 AS theKey, COUNT(*) AS sourceCount FROM ${hiveconf:CNL_SOURCE_TBL_NAME}) AS tbl1
JOIN
(SELECT 1 AS theKey, COUNT(*) AS destCount FROM ${hiveconf:OCNL_SOURCE_TBL}) AS tbl2
ON tbl1.theKey = tbl2.theKey ;

-- -----------------------------------------------------------------------------
-- CNL_FILE_META_TBL
-- -----------------------------------------------------------------------------
-- QA table for auditing clean nom log file output.
SET CNL_FILE_META_TBL_NAME=clean_nom_log_meta ;
-- This has line counts per file output by Nom Clean.
-- Possible alarm for services if large drops or spikes occur over single days.
DROP TABLE IF EXISTS ${CNL_FILE_META_TBL_NAME};
CREATE TABLE ${CNL_FILE_META_TBL_NAME} STORED AS SEQUENCEFILE AS 
SELECT 
 tbl.sourceFileAbsolutePath,
 tbl.fileName,
 COUNT(*) AS lineCount 
FROM (
 SELECT
  INPUT__FILE__NAME AS sourceFileAbsolutePath,
  SPLIT(INPUT__FILE__NAME,'clean/')[1] AS fileName
 FROM ${hiveconf:CNL_SOURCE_TBL_NAME}
) AS tbl GROUP BY  tbl.sourceFileAbsolutePath, tbl.fileName ORDER BY tbl.fileName ASC ;

-- -----------------------------------------------------------------------------
-- OCLN_SOURCE_EVENT_COUNT_TBL
-- -----------------------------------------------------------------------------
-- QA table for auditing clean nom log file output at the event per day level.
SET OCLN_SOURCE_EVENT_COUNT_TBL_NAME=clean_nom_log_event_counts ;
SET OCLN_SOURCE_EVENT_COUNT_TBL_S3_LOC=s3://data.sitecues.com/telem/hive/clean_nom_log_event_counts ;
-- This has source events counts per file output by Nom Clean.
-- Possible alarm for services if large drops or spikes occur over single days.
DROP TABLE IF EXISTS ${hiveconf:OCLN_SOURCE_EVENT_COUNTS};
CREATE EXTERNAL TABLE ${hiveconf:OCLN_SOURCE_EVENT_COUNTS} (
 fileName STRING,
 name STRING,
 occuranceCount BIGINT
) STORED AS TEXTFILE 
LOCATION ${hiveconf:OCLN_SOURCE_EVENT_COUNT_TBL_S3_LOC} ;
-- POPULATE clean_nom_log_event_counts
INSERT OVERWRITE TABLE ${hiveconf:OCLN_SOURCE_EVENT_COUNTS}
SELECT 
 fileName,
 name,
 COUNT(*) AS occuranceCount
FROM (
 SELECT 
  fileName, 
  get_json_object(line, '$.name') AS name
 FROM ${hiveconf:OCNL_SOURCE_TBL}
) AS tbl group by fileName,name ORDER BY fileName ASC, occuranceCount DESC ;


-- -----------------------------------------------------------------------------
-- CHECK CONSISTENCY OF 
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS is_optimized_clean_nom_log_and_clean_nom_log_json_consistent;
CREATE EXTERNAL TABLE is_optimized_clean_nom_log_and_clean_nom_log_json_consistent (
 optomizedCount BIGINT,
 jsonCount BIGINT,
 isConsistent BOOLEAN
) STORED AS TEXTFILE 
location 's3://data.sitecues.com/telem/hive/is_optimized_clean_nom_log_and_clean_nom_log_json_consistent' ;
INSERT OVERWRITE TABLE is_optimized_clean_nom_log_and_clean_nom_log_json_consistent
SELECT 
 optimized_clean_nom_logCount AS optomizedCount,
 clean_nom_log_jsonCount AS jsonCount,
 (optimized_clean_nom_logCount = clean_nom_log_jsonCount) AS isConsistent
FROM
(
 SELECT 1 AS aKey, COUNT(*) AS optimized_clean_nom_logCount FROM optimized_clean_nom_log
) AS tbl1 JOIN ( 
 SELECT 1 AS aKey, COUNT(*) AS clean_nom_log_jsonCount FROM clean_nom_log_json
) AS tbl2 ON tbl1.aKey = tbl2.aKey ;






