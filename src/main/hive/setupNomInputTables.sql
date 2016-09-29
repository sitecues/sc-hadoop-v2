-- Create clean_nom_log_json and check data for consistency. Works off of clean metrics data.

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- VARIABLES for SC Hive.
--------------------------------------------------------------------------------
-- SAMPLE Rate for ${OCNL_SOURCE_TBL} (OCLN=Optomized Clean Nom Log)
-- NOTE this is an integer between 1 and 100 percent. USE 100 to disable sampling.
SET SAMPLE_RATE=10;
--------------------------------------------------------------------------------
-- THE Clean Nom Log SOURCE TABLE 
SET CNL_SOURCE_S3_LOCATION=s3://data.sitecues.com/telem/nom/clean/ ;
SET CNL_SOURCE_TBL_NAME=clean_nom_log_source;
--------------------------------------------------------------------------------
-- The Optomised Clean nom log table name
SET OCNL_SOURCE_TBL=optimized_clean_nom_log ;

-- -----------------------------------------------------------------------------
-- CREATE THE Clean Nom Log Source template over the existing raw clean data from nom on s3.
-- This table is the entire data set of clean nom logs. It should not be queried directly as all 
-- subsequent tables will be built off the ${CNL_TBL_NAME} table with the apropriate sample rate.
DROP TABLE IF EXISTS ${hiveconf:CNL_SOURCE_TBL_NAME};
CREATE EXTERNAL TABLE ${hiveconf:CNL_SOURCE_TBL_NAME} (line string) location '${hiveconf:CNL_SOURCE_S3_LOCATION}';

-- -----------------------------------------------------------------------------
-- Set up optimized_clean_nom_log (an optomized version of the clean_nom_log table)
-- * optimized_clean_nom_log *
DROP TABLE IF EXISTS ${hiveconf:OCNL_SOURCE_TBL};
-- Because we use sampeling by percentage and that is not supported by our set 
-- hive.input.format=org.apache.hadoop.hive.ql.io.HiveCombineSplitsInputFormat;
-- We have to store the current setting, make the sample query and then restore
-- the settings for subsequent queries.
SET OLD_HIVE_INPUT_FORMAT=${hive.input.format} ;
SET OLD_TEZ_INPUT_FORMAT=${hive.tez.input.format} ;
-- SET hive.input.format and hive.tez.input.format to a inpunt format that 
-- supports percentage based sampeling.
SET hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.tez.input.format=${hive.input.format};
-- CREATE THE TABLE  ${hiveconf:OCNL_SOURCE_TBL} Modle.
CREATE TABLE ${hiveconf:OCNL_SOURCE_TBL} (
 sourceFileAbsolutePath STRING,
 fileName STRING, 
 line STRING 
) STORED AS ORC ;
--  Insert data at the set sample rate
INSERT OVERWRITE TABLE ${hiveconf:OCNL_SOURCE_TBL} SELECT 
 INPUT__FILE__NAME AS sourceFileAbsolutePath,
 SPLIT(INPUT__FILE__NAME,'clean/')[1] AS fileName, 
 line 
FROM ${hiveconf:CNL_SOURCE_TBL_NAME} TABLESAMPLE(${hiveconf:SAMPLE_RATE} PERCENT) s ; 
-- -----------------------------------------------------------------------------

-- -----------------------------------------------------------------------------
SET OCNLJ_STAGE_TBL_NAME=clean_nom_log_json_stage;
SET OCNLJ_STAGE_TBL_LOC=s3://data.sitecues.com/telem/hive/clean_nom_log_json;

-- -----------------------------------------------------------------------------
-- STAGE the JSON data in good size chunks with compression and remove the stage table.
-- Similar to clean_nom_log, but uses snappy instead of gzip for better access and stores externally.
-- * clean_nom_log_json_stage *
DROP TABLE IF EXISTS ${hiveconf:OCNLJ_STAGE_TBL_NAME} ;
CREATE EXTERNAL TABLE ${hiveconf:OCNLJ_STAGE_TBL_NAME} (
 line String
)STORED AS TEXTFILE
LOCATION '${hiveconf:OCNLJ_STAGE_TBL_LOC}' ;
-- POPULATE ${hiveconf:OCNL_SOURCE_TBL}
INSERT OVERWRITE TABLE ${hiveconf:OCNLJ_STAGE_TBL_NAME} SELECT line FROM ${hiveconf:OCNL_SOURCE_TBL};
DROP TABLE IF EXISTS ${hiveconf:OCNLJ_STAGE_TBL_NAME} ;

-- -----------------------------------------------------------------------------
-- -----------------------------------------------------------------------------
-- -----------------------------------------------------------------------------
-- CLEAN NOM JSON!
-- -----------------------------------------------------------------------------
SET OCLN_JSON_TBL_NAME=clean_nom_log_json ;
SET OCLN_JSON_TBL_S3_LOC=${hiveconf:OCNLJ_STAGE_TBL_LOC} ;

-- -----------------------------------------------------------------------------
-- Create the actual json read table from the staged data.
-- * clean_nom_log_json --
ADD JAR s3://prd.emr.sitecues.com/serde/json-serde-1.3.7-jar-with-dependencies.jar ;
DROP TABLE IF EXISTS ${hiveconf:OCLN_JSON_TBL_NAME} ;
CREATE EXTERNAL TABLE ${hiveconf:OCLN_JSON_TBL_NAME} (
    clientLanguage STRING,
    clientTimeMs BIGINT,
    details STRUCT< 
        isRetina : BOOLEAN,
        nativeZoom : FLOAT
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
    zoomLevel FLOAT
) ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
location '${hiveconf:OCLN_JSON_TBL_S3_LOC}' ;

-- -----------------------------------------------------------------------------
-- -----------------------------------------------------------------------------
-- OCNL_EXPLODE_TBL
-- An example of explosion for ua groups and sudo events.
SET OCNL_EXPLODE_TBL_NAME=test_view_clean_nom_explosion ;
DROP TABLE IF EXISTS ${hiveconf:OCNL_EXPLODE_TBL_NAME} ;
CREATE TABLE ${hiveconf:OCNL_EXPLODE_TBL_NAME} STORED AS ORC AS
SELECT 
 year(FROM_UNIXTIME(CEIL(serverTs/1000))) AS year,
 month(FROM_UNIXTIME(CEIL(serverTs/1000))) AS month,
 day(FROM_UNIXTIME(CEIL(serverTs/1000))) AS day,
 serverTs,
 siteid, 
 userId,
 sessionId,
 pageViewId,
 name AS eventName,
 location,
 pseudoEvenent,
 uaGroup
FROM ${hiveconf:OCLN_JSON_TBL_NAME} LATERAL VIEW explode(meta.locations) adTable AS location 
LATERAL VIEW explode(meta.pseudoevents) abTable AS pseudoEvenent 
LATERAL VIEW explode(meta.ua.groups) acTable AS uaGroup ;

-- -----------------------------------------------------------------------------
-- -----------------------------------------------------------------------------

CREATE TABLE test_view_reduced_001_explosion STORED AS ORC AS
SELECT 
 year,
 month,
 day,
 siteId,
 COUNT(DISTINCT(userId)) as uniqueUserIds,
 COUNT(DISTINCT(sessionId)) as uniqueSessionIds,
 COUNT(DISTINCT(pageViewId)) as uniquePageViewIds,
 location,
 pseudoEvenent AS pseudoEvent,
 uaGroup,
 COUNT(1) AS totalOccurances
 FROM ${hiveconf:OCNL_EXPLODE_TBL_NAME} GROUP BY year,month,day,siteId,location,pseudoEvenent,uaGroup ;



-- -----------------------------------------------------------------------------
-- -----------------------------------------------------------------------------
-- SET VARIABLES TO ALLOW HIVE TO HANDLE THE PARTITIONING OF THE NEXT INSERT
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
-- -----------------------------------------------------------------------------
-- clean_nom_id_audit_stage_001 is a partitioned table on year / month
DROP TABLE IF EXISTS clean_nom_id_audit_stage_001 ;
CREATE TABLE clean_nom_id_audit_stage_001  (
 day INT,
 hour INT,
 minute INT,
 second INT,
 siteid STRING,
 userId STRING,
 sessionId STRING,
 pageViewId STRING,
 eventName STRING,
 uaGroups ARRAY<STRING>,
 pseudoevents ARRAY<STRING>
) PARTITIONED BY (year INT ,month INT) STORED AS ORC ;
-- -----------------------------------------------------------------------------
-- Insert partitioned data into clean_nom_id_audit_stage_001.
INSERT INTO clean_nom_id_audit_stage_001 PARTITION (year,month) 
SELECT 
 day,
 hour,
 minute,
 second,
 siteid,
 userId,
 sessionId,
 pageViewId,
 eventName,
 uaGroups,
 pseudoevents,
 year,
 month
FROM (
 SELECT
  YEAR(unixTs) AS year,
  MONTH(unixTs) AS month,
  DAY(unixTs) AS day,
  HOUR(unixTs) AS hour,
  MINUTE(unixTs) AS minute,
  SECOND(unixTs) AS second,
  siteid,
  userId,
  sessionId,
  pageViewId,
  eventName,
  uaGroups,
  pseudoevents
 FROM (
  SELECT 
   FROM_UNIXTIME(CEIL(serverTs/1000)) AS unixTs,
   siteid, 
   userId,
   sessionId,
   pageViewId,
   name AS eventName,
   meta.ua.groups AS uaGroups,
   meta.pseudoevents AS pseudoevents
  FROM clean_nom_log_json 
 ) tbl
) AS sourceTbl ;
-- END PARTITIONED OPERATION.
-- -----------------------------------------------------------------------------

-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS clean_nom_id_audit_stage_002 ;
CREATE TABLE clean_nom_id_audit_stage_002 AS 
SELECT 
 year,
 month,
 day,
 siteId,
 eventname, 
 COUNT(DISTINCT(pageviewid)) AS pageViewIds,
 COUNT(DISTINCT(userId)) AS uniqueUserIds,
 COUNT(DISTINCT(sessionid)) AS uniqueSessionIds
FROM clean_nom_id_audit_stage_001 
GROUP BY year,month,day,siteId,eventName ;
-- -----------------------------------------------------------------------------
-- -----------------------------------------------------------------------------

ADD JAR s3://prd.emr.sitecues.com/udf/brickhouse-0.7.1-SNAPSHOT.jar ;
create temporary function collect as 'brickhouse.udf.collect.CollectUDAF';

CREATE TABLE session_event_counts_by_day STORED AS ORC AS
SELECT year, month,day, b.sessionId, collect(eventMap) as eventCountMap
FROM
(
    SELECT year, month, day, a.sessionId, map(a.eventName,a.cnt) as eventMap
    FROM
    ( 
         SELECT year,month,day,
                sessionId,
                eventName, 
                count(1)  as cnt
         FROM test_view_clean_nom_explosion
         GROUP BY
                year,month,day,
                sessionId,
                eventName
    )a
)b
GROUP BY year, month, day, b.sessionId;

CREATE TABLE session_event_counts_by_month STORED AS ORC AS
SELECT year, month, b.sessionId, collect(eventMap) as eventCountMap
FROM
(
    SELECT year, month, a.sessionId, map(a.eventName,a.cnt) as eventMap
    FROM
    ( 
         SELECT year,month,
                sessionId,
                eventName, 
                count(1)  as cnt
         FROM test_view_clean_nom_explosion
         GROUP BY
                year,month,
                sessionId,
                eventName
    )a
)b
GROUP BY year, month, b.sessionId;