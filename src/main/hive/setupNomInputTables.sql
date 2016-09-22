
-- Clean up any crap from dev
DROP TABLE IF EXISTS clean_nom_log;
DROP TABLE IF EXISTS optomized_clean_nom_log;
DROP TABLE IF EXISTS optimized_clean_nom_log;
DROP TABLE IF EXISTS packed_raw_nom_log;
DROP TABLE IF EXISTS raw_metrics_log;
DROP TABLE IF EXISTS clean_nom_log_metz;
DROP TABLE IF EXISTS clean_nom_log_meta;
DROP TABLE IF EXISTS clean_nom_log_event_counts;

-- -----------------------------------------------------------------------------
-- SET up a table where each row is simply a single entry with a json string representing a single event
-- * clean_nom_log - gzipped *
DROP TABLE IF EXISTS clean_nom_log;
CREATE EXTERNAL TABLE clean_nom_log (line string) location 's3://data.sitecues.com/telem/nom/clean/';

-- -----------------------------------------------------------------------------
-- Set up optimized_clean_nom_log (an optomized version of the clean_nom_log table)
-- * optimized_clean_nom_log *
DROP TABLE IF EXISTS optimized_clean_nom_log;
CREATE TABLE optimized_clean_nom_log STORED AS SEQUENCEFILE AS 
SELECT 
 INPUT__FILE__NAME AS sourceFileAbsolutePath,
 SPLIT(INPUT__FILE__NAME,'clean/')[1] AS fileName, 
 line 
FROM clean_nom_log; 

-- -----------------------------------------------------------------------------
-- Consistency check in event counts between clean_nom_log and optimized_clean_nom_log
-- * is_optimized_clean_nom_log_consistent *
DROP TABLE IF EXISTS is_optimized_clean_nom_log_consistent;
CREATE TABLE is_optimized_clean_nom_log_consistent STORED AS SEQUENCEFILE AS 
SELECT
 sourceCount,
 optimizedCount,
 (sourceCount=optimizedCount) AS isConsistent
FROM 
(SELECT 1 AS theKey, COUNT(*) AS sourceCount FROM clean_nom_log) AS tbl1
JOIN
(SELECT 1 AS theKey, COUNT(*) AS optimizedCount FROM optimized_clean_nom_log) AS tbl2
ON tbl1.theKey = tbl2.theKey ;

-- -----------------------------------------------------------------------------
-- SET UP THE metadata table for the data in the raw clean nom logs. Currently includes only the event count per day.
-- * clean_nom_log_meta *
DROP TABLE IF EXISTS clean_nom_log_meta;
CREATE TABLE clean_nom_log_meta STORED AS SEQUENCEFILE AS 
SELECT 
 tbl.sourceFileAbsolutePath,
 tbl.fileName,
 COUNT(*) AS lineCount 
FROM (
 SELECT
  INPUT__FILE__NAME AS sourceFileAbsolutePath,
  SPLIT(INPUT__FILE__NAME,'clean/')[1] AS fileName
 FROM clean_nom_log
) AS tbl GROUP BY  tbl.sourceFileAbsolutePath, tbl.fileName ORDER BY tbl.fileName ASC ;

-- -----------------------------------------------------------------------------
-- Set up an event counts per event
-- * clean_nom_log_event_counts *
DROP TABLE IF EXISTS clean_nom_log_event_counts;
CREATE EXTERNAL TABLE clean_nom_log_event_counts 
(
 fileName STRING,
 name STRING,
 occuranceCount BIGINT
) STORED AS TEXTFILE 
LOCATION 's3://data.sitecues.com/telem/hive/clean_nom_log_event_counts' ;
-- POPULATE clean_nom_log_event_counts
INSERT OVERWRITE TABLE clean_nom_log_event_counts
SELECT 
 fileName,
 name,
 COUNT(*) AS occuranceCount
FROM (
 SELECT 
  fileName, 
  get_json_object(line, '$.name') AS name
 FROM optimized_clean_nom_log
) AS tbl group by fileName,name ORDER BY fileName ASC, occuranceCount DESC ;


-- -----------------------------------------------------------------------------
-- STAGE the JSON data in good size chunks with compression. 
-- Similar to clean_nom_log, but uses snappy instead of gzip for better access and stores externally.
-- * clean_nom_log_json_stage *
DROP TABLE IF EXISTS clean_nom_log_json_stage ;
CREATE EXTERNAL TABLE clean_nom_log_json_stage (
 line String
)STORED AS TEXTFILE
LOCATION 's3://data.sitecues.com/telem/hive/clean_nom_log_json' ;
-- POPULATE clean_nom_log_json_stage
INSERT OVERWRITE TABLE clean_nom_log_json_stage SELECT line FROM clean_nom_log;

-- -----------------------------------------------------------------------------
-- Create the actual json read table from the staged data.
-- * clean_nom_log_json --
ADD JAR s3://prd.emr.sitecues.com/serde/json-serde-1.3.7-jar-with-dependencies.jar ;
DROP TABLE IF EXISTS clean_nom_log_json ;
CREATE EXTERNAL TABLE clean_nom_log_json
(
    clientLanguage STRING,
    clientTimeMs BIGINT,
    details STRUCT< 
        isRetina : BOOLEAN,
        nativeZoom : DOUBLE
    >,
    eventId STRING,
    meta STRUCT <
        domain: STRING,
        locations : ARRAY<STRING>,
        pseudoEvents: ARRAY<STRING>,
        ua : STRUCT<
            browser : STRING,
            browserVersion: DOUBLE,
            groups : ARRAY<STRING>
        >
    >,
    name  STRING,
    pageViewId STRING,
    serverTs BIGINT,
    sessionId STRING,
    siteId STRING,
    ttsState  BOOLEAN,
    userId STRING,
    zoomLevel DOUBLE
) ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
location 's3://data.sitecues.com/telem/hive/clean_nom_log_json' ;

-- -----------------------------------------------------------------------------
-- CHECK CONSISTENCY OF 
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
