-- Create clean_nom_log_json and check data for consistency. Works off of clean metrics data.

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- VARIABLES for SC Hive.
--------------------------------------------------------------------------------
-- SAMPLE Rate for ${OCNL_SOURCE_TBL} (OCLN=Optomized Clean Nom Log)
-- NOTE this is an integer between 1 and 100 percent. USE 100 to disable sampling.
SET SAMPLE_RATE=100;
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
SELECT 'ATTEMPTING POPULATION OF ${hiveconf:OCNL_SOURCE_TBL}';
--  Insert data at the set sample rate
INSERT OVERWRITE TABLE ${hiveconf:OCNL_SOURCE_TBL} SELECT 
 INPUT__FILE__NAME AS sourceFileAbsolutePath,
 SPLIT(INPUT__FILE__NAME,'clean/')[1] AS fileName, 
 line 
FROM ${hiveconf:CNL_SOURCE_TBL_NAME} TABLESAMPLE(${hiveconf:SAMPLE_RATE} PERCENT) s ; 
SELECT 'COMPLETED POPULATION OF ${hiveconf:OCNL_SOURCE_TBL}';
ANALYZE TABLE ${hiveconf:OCNL_SOURCE_TBL} COMPUTE STATISTICS;
ANALYZE TABLE ${hiveconf:OCNL_SOURCE_TBL} COMPUTE STATISTICS for columns;
SELECT 'COMPLETED ANALYSIS OF ${hiveconf:OCNL_SOURCE_TBL}';
-- LOG SELECT
-- -----------------------------------------------------------------------------



-- -----------------------------------------------------------------------------
SET OCNLJ_STAGE_TBL_NAME=clean_nom_log_json_stage;
SET OCNLJ_STAGE_TBL_LOC=s3://data.sitecues.com/telem/hive/clean_nom_log_json;
SET OCNLJ_STAGE_TBL_SOURCE_TBL=${hiveconf:OCNL_SOURCE_TBL} ;
SELECT 'ATTEMPTING ${hiveconf:OCNLJ_STAGE_TBL_NAME} BUILD';
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
SELECT 'ATTEMPTING POPULATION OF ${hiveconf:OCNLJ_STAGE_TBL_NAME}';
INSERT OVERWRITE TABLE ${hiveconf:OCNLJ_STAGE_TBL_NAME} SELECT line FROM ${hiveconf:OCNLJ_STAGE_TBL_SOURCE_TBL};
-- LOG SELECT
SELECT 'COMPLETED POPULATION OF ${hiveconf:OCNLJ_STAGE_TBL_NAME}';
DROP TABLE IF EXISTS ${hiveconf:OCNLJ_STAGE_TBL_NAME} ;
SELECT 'REMOVED ${hiveconf:OCNLJ_STAGE_TBL_NAME}, DATA IS NOW COALATED AT ${hiveconf:OCNLJ_STAGE_TBL_LOC}';

-- -----------------------------------------------------------------------------
-- -----------------------------------------------------------------------------
-- -----------------------------------------------------------------------------
-- CLEAN NOM JSON!
-- -----------------------------------------------------------------------------
SET OCLN_JSON_TBL_NAME=clean_nom_log_json ;
SET OCLN_JSON_TBL_S3_LOC=${hiveconf:OCNLJ_STAGE_TBL_LOC} ;
SELECT 'ATTEMPTING ${hiveconf:OCLN_JSON_TBL_NAME} BUILD. SOURCE ${hiveconf:OCLN_JSON_TBL_S3_LOC}';
-- -----------------------------------------------------------------------------
-- Create the actual json read table from the staged data.
-- * clean_nom_log_json --
ADD JAR s3://prd.emr.sitecues.com/serde/json-serde-1.3.7-jar-with-dependencies.jar ;
DROP TABLE IF EXISTS ${hiveconf:OCLN_JSON_TBL_NAME} ;
SELECT 'ATTEMPTING CREATE ${hiveconf:OCLN_JSON_TBL_NAME}';

CREATE EXTERNAL TABLE ${hiveconf:OCLN_JSON_TBL_NAME} (
badgeHeight INT,
    badgeLeft INT,
    badgePalette STRING,
    badgeTop INT,
    dateIndex INT,
    clientLanguage STRING,
    clientTimeMs BIGINT,
    details STRUCT< 
        isRetina: BOOLEAN,
        nativeZoom: FLOAT,
        browser: STRING,
        browserVersion: INT,
        error: STRING,
        isRetina: BOOLEAN,
        nativeZoom: FLOAT,
        navPlatform: STRING,
        os: STRING,
        osVersion: FLOAT
    >,
    has MAP<STRING,BOOLEAN>,
    eventId STRING,
    meta STRUCT <
        domain: STRING,
        locations : ARRAY<STRING>,
        events: ARRAY<STRING>,
        ua : STRUCT<
            browser : STRING,
            browserVersion: DOUBLE,
            ids: ARRAY<STRING>
        >
    >,
    isClassicMode BOOLEAN,
    sensitivity FLOAT,
    source STRING,
    version STRING,
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
SELECT 'SUCCESS CREATE ${hiveconf:OCLN_JSON_TBL_NAME}';

SELECT 'ANALYSING ${hiveconf:OCLN_JSON_TBL_NAME}';
-- GATHER TEZ STATS FOR table QUERY PLANNER.
ANALYZE TABLE ${hiveconf:OCLN_JSON_TBL_NAME} COMPUTE STATISTICS;
-- CAN NOT GATHER STATS FOR COLUMN STRUCTS. YET!

-- -----------------------------------------------------------------------------
-- -----------------------------------------------------------------------------
-- BEGIN OCNL_EXPLODE_STAGE_BY_DATEINDEX_TBL_NAME
-- Pull all fields from raw serde JSON into ORC for effiecient vectorized explosion.
-- Partitions executed on dateIndex
-- SET TABLE NAME AS VAR
SET OCNL_EXPLODE_STAGE_BY_DATEINDEX_TBL_NAME=clean_nom_explosion_by_dateindex_map_stage ;
SET OCNL_EXPLODE_STAGE_BY_DATEINDEX_TBL_SOURCE_TBL_NAME=${hiveconf:OCLN_JSON_TBL_NAME} ;

SELECT 'ATTEMPTING ${hiveconf:OCNL_EXPLODE_STAGE_BY_DATEINDEX_TBL_NAME} BUILD';
-- ENSURE TABLE DOES NOT ALREADY EXIST
DROP TABLE IF EXISTS ${hiveconf:OCNL_EXPLODE_STAGE_BY_DATEINDEX_TBL_NAME} ;
-- CREATE TABLE
CREATE TABLE ${hiveconf:OCNL_EXPLODE_STAGE_BY_DATEINDEX_TBL_NAME} (
 serverTs BIGINT,
 siteid STRING, 
 userId STRING,
 sessionId STRING,
 pageViewId STRING,
 eventName STRING,
 locations ARRAY<STRING>,
 pseudoEvents ARRAY<STRING>,
 uaGroups ARRAY<STRING>
) PARTITIONED BY (dateIndex INT) STORED AS ORC ;
SELECT 'LOADING DATA INTO ${hiveconf:OCNL_EXPLODE_STAGE_BY_DATEINDEX_TBL_NAME} FROM ';
-- LOAD DATA INTO ${hiveconf:OCNL_EXPLODE_STAGE_BY_DATEINDEX_TBL_NAME}
INSERT OVERWRITE TABLE ${hiveconf:OCNL_EXPLODE_STAGE_BY_DATEINDEX_TBL_NAME}
PARTITION (dateIndex)
SELECT 
 serverTs, 
 siteid, 
 userId,
 sessionId,
 pageViewId,
 name AS eventName,
 meta.locations AS locations,
 meta.events AS pseudoEvents,
 meta.ua.ids AS uaGroups,
 dateIndex
FROM ${hiveconf:OCNL_EXPLODE_STAGE_BY_DATEINDEX_TBL_SOURCE_TBL_NAME} ;
-- GATHER TEX STATS FOR QUERY PLANNER.
ANALYZE TABLE ${hiveconf:OCNL_EXPLODE_STAGE_TBL_NAME} PARTITION (dateIndex) COMPUTE STATISTICS;
ANALYZE TABLE ${hiveconf:OCNL_EXPLODE_STAGE_TBL_NAME} PARTITION (dateIndex) COMPUTE STATISTICS for columns siteId,userId,sessionId,pageViewId,eventName;
-- LOG SELECT
SELECT 'COMPLETED POPULATION OF ${hiveconf:OCNL_EXPLODE_STAGE_BY_DATEINDEX_TBL_NAME}';
-- END OCNL_EXPLODE_STAGE_TBL
-- -----------------------------------------------------------------------------
-- -----------------------------------------------------------------------------





-- BEGIN OCNL_EXPLODE_TBL DATE INDEX ONLY
-- An example of explosion for ua.all groups and sudo events.
-- SET TABLE NAME AS VAR
SET OCNL_EXPLODE_BY_DATEINDEX_TBL_NAME=clean_nom_explosion_by_dateindex_map ;
-- ENSURE TABLE DOES NOT EXIST
DROP TABLE IF EXISTS ${hiveconf:OCNL_EXPLODE_BY_DATEINDEX_TBL_NAME} ;
-- CREATE TABLE
CREATE TABLE ${hiveconf:OCNL_EXPLODE_BY_DATEINDEX_TBL_NAME} (
 serverTs BIGINT,
 siteid STRING, 
 userId STRING,
 sessionId STRING,
 pageViewId STRING,
 eventName STRING,
 location STRING,
 pseudoEvent STRING,
 uaGroup STRING
) PARTITIONED BY (dateIndex INT) STORED AS ORC ;
-- LOAD DATA INTO ${hiveconf:OCNL_EXPLODE_TBL_NAME}
INSERT OVERWRITE TABLE ${hiveconf:OCNL_EXPLODE_BY_DATEINDEX_TBL_NAME} 
PARTITION (dateIndex)
SELECT 
 serverTs,
 siteid, 
 userId,
 sessionId,
 pageViewId,
 eventName,
 location,
 pseudoEvent,
 uaGroup,
 dateIndex
FROM ${hiveconf:OCNL_EXPLODE_STAGE_BY_DATEINDEX_TBL_NAME} LATERAL VIEW explode(locations) adTable AS location 
LATERAL VIEW explode(pseudoEvents) abTable AS pseudoEvent 
LATERAL VIEW explode(uaGroups) acTable AS uaGroup ;
-- GATHER TEZ STATS FOR QUERY PLANNER.
ANALYZE TABLE ${hiveconf:OCNL_EXPLODE_BY_DATEINDEX_TBL_NAME} PARTITION (dateIndex) COMPUTE STATISTICS;
ANALYZE TABLE ${hiveconf:OCNL_EXPLODE_BY_DATEINDEX_TBL_NAME} PARTITION (dateIndex) COMPUTE STATISTICS for columns;
-- LOG SELECT
SELECT 'COMPLETED POPULATION OF ${hiveconf:OCNL_EXPLODE_BY_DATEINDEX_TBL_NAME}';

-- -----------------------------------------------------------------------------
-- -----------------------------------------------------------------------------
-- Roll up sessions by dateindex,location,pseudoEvent,uaGroup 
SET DAILY_SESSION_ROLLUP_BY_DATEIDEX_TBL_NAME=daily_session_by_dateindex_rollup ;
-- ENSURE TABLE DOES NOT ALREADY EXIST
DROP TABLE IF EXISTS ${hiveconf:DAILY_SESSION_ROLLUP_BY_DATEIDEX_TBL_NAME} ;
-- CREATE TABLE / LOAD DATA
CREATE TABLE ${hiveconf:DAILY_SESSION_ROLLUP_BY_DATEIDEX_TBL_NAME} STORED AS ORC AS
SELECT 
 dateIndex,
 COUNT(DISTINCT(userId)) as uniqueUserIds,
 COUNT(DISTINCT(sessionId)) as uniqueSessionIds,
 COUNT(DISTINCT(pageViewId)) as uniquePageViewIds,
 location,
 pseudoEvent,
 uaGroup,
 COUNT(1) AS totalOccurances
 FROM ${hiveconf:OCNL_EXPLODE_BY_DATEINDEX_TBL_NAME} GROUP BY dateIndex,location,pseudoEvent,uaGroup ;


-- -----------------------------------------------------------------------------
-- -----------------------------------------------------------------------------
-- -----------------------------------------------------------------------------
-- BEGIN DAILY LDJSON OUTPUT FOR NOM CLIENT
-- The base s3 location all ldjson will be output under
SET DAILY_NOM_S3_BASE_LOC=s3://data.sitecues.com/telem/hive/output/nom/daily/ ;
SET DAILY_NOM_SOURCE_TBL_NAME=${hiveconf:DAILY_SESSION_ROLLUP_BY_DATEIDEX_TBL_NAME} ;

-- EVENT vars
SET DAILY_NOM_EVENT_OUT_STAGE_TBL=internal_event_telem_json_out ;
SET DAILY_NOM_EVENT_OUT_S3_LOC=${hiveconf:DAILY_NOM_S3_BASE_LOC}event.ldjson ;
-- USER vars
SET DAILY_NOM_USER_OUT_STAGE_TBL=internal_user_telem_json_out ;
SET DAILY_NOM_USER_OUT_S3_LOC=${hiveconf:DAILY_NOM_S3_BASE_LOC}user.ldjson ;
-- SESSION vars
SET DAILY_NOM_SESSION_OUT_STAGE_TBL=internal_session_telem_json_out ;
SET DAILY_NOM_SESSION_OUT_S3_LOC=${hiveconf:DAILY_NOM_S3_BASE_LOC}session.ldjson ;
-- Ensure DAILY NOM LDJSON STAGE Tables do not exist
DROP TABLE IF EXISTS ${hiveconf:DAILY_NOM_EVENT_OUT_STAGE_TBL} ;
DROP TABLE IF EXISTS ${hiveconf:DAILY_NOM_USER_OUT_STAGE_TBL} ;
DROP TABLE IF EXISTS ${hiveconf:DAILY_NOM_SESSION_OUT_STAGE_TBL} ;
-- STAGE DAILY NOM EVENT LDJSON
CREATE TABLE ${hiveconf:DAILY_NOM_EVENT_OUT_STAGE_TBL} AS
  SELECT 
   tjson(map(eventName,BH_union(dateEntry))) AS entry
  FROM (
   SELECT
    CONCAT(location,'||',uaGroup,'||',pseudoEvent) AS eventName,map(dateIndex,totalOccurances) AS dateEntry
   FROM ${hiveconf:DAILY_NOM_SOURCE_TBL_NAME}
 ) tbl GROUP BY eventName ;
-- LOG OUTPUT
SELECT CONCAT('STAGED ',COUNT(1),' rows into ${hiveconf:DAILY_NOM_EVENT_OUT_STAGE_TBL}') FROM ${hiveconf:DAILY_NOM_EVENT_OUT_STAGE_TBL};
-- STAGE DAILY NOM USER LDJSON
CREATE TABLE ${hiveconf:DAILY_NOM_USER_OUT_STAGE_TBL} AS
 SELECT 
   tjson(map(eventName,BH_union(dateEntry))) AS entry
  FROM (
   SELECT
    CONCAT(location,'||',uaGroup,'||',pseudoEvent) AS eventName,map(dateIndex,uniqueUserIds) AS dateEntry
   FROM ${hiveconf:DAILY_NOM_SOURCE_TBL_NAME}
 ) tbl GROUP BY eventName ;
-- LOG OUTPUT
SELECT CONCAT('STAGED ',COUNT(1),' rows into ${hiveconf:DAILY_NOM_USER_OUT_STAGE_TBL}') FROM ${hiveconf:DAILY_NOM_USER_OUT_STAGE_TBL};
-- STAGE DAILY NOM SESSION LDJSON
CREATE TABLE ${hiveconf:DAILY_NOM_SESSION_OUT_STAGE_TBL} AS
  SELECT 
   tjson(map(eventName,BH_union(dateEntry))) AS entry
  FROM (
   SELECT
    CONCAT(location,'||',uaGroup,'||',pseudoEvent) AS eventName,map(dateIndex,uniqueSessionIds) AS dateEntry
   FROM ${hiveconf:DAILY_NOM_SOURCE_TBL_NAME}
 ) tbl GROUP BY eventName;
-- LOG OUTPUT
SELECT CONCAT('STAGED ',COUNT(1),' rows into ${hiveconf:DAILY_NOM_SESSION_OUT_STAGE_TBL}') FROM ${hiveconf:DAILY_NOM_SESSION_OUT_STAGE_TBL};
-- BEGIN OUTPUT DAILY NOM EVENT LDJSON
SET hive.exec.compress.output=false;
SELECT 'OUTPUTING contents of ${hiveconf:DAILY_NOM_EVENT_OUT_STAGE_TBL} to ${hiveconf:DAILY_NOM_EVENT_OUT_S3_LOC}' ;
INSERT OVERWRITE DIRECTORY '${hiveconf:DAILY_NOM_EVENT_OUT_S3_LOC}' SELECT entry FROM ${hiveconf:DAILY_NOM_EVENT_OUT_STAGE_TBL} ; 
-- OUTPUT DAILY NOM USER LDJSON
SELECT 'OUTPUTING contents of ${hiveconf:DAILY_NOM_USER_OUT_S3_LOC} to ${hiveconf:DAILY_NOM_USER_OUT_S3_LOC}' ;
INSERT OVERWRITE DIRECTORY '${hiveconf:DAILY_NOM_USER_OUT_S3_LOC}' SELECT entry FROM ${hiveconf:DAILY_NOM_USER_OUT_STAGE_TBL} ; 
-- OUTPUT DAILY NOM SESSION LDJSON
SELECT 'OUTPUTING contents of ${hiveconf:DAILY_NOM_SESSION_OUT_S3_LOC} to ${hiveconf:DAILY_NOM_SESSION_OUT_S3_LOC}' ;
INSERT OVERWRITE DIRECTORY '${hiveconf:DAILY_NOM_SESSION_OUT_S3_LOC}' SELECT entry FROM ${hiveconf:DAILY_NOM_SESSION_OUT_STAGE_TBL} ; 
SET hive.exec.compress.output=true;
-- END OUTPUT DAILY NOM EVENT LDJSON

-- CLEANUP Ensure DAILY NOM LDJSON STAGE Tables do not exist
DROP TABLE IF EXISTS ${hiveconf:DAILY_NOM_EVENT_OUT_STAGE_TBL} ;
DROP TABLE IF EXISTS ${hiveconf:DAILY_NOM_USER_OUT_STAGE_TBL} ;
DROP TABLE IF EXISTS ${hiveconf:DAILY_NOM_SESSION_OUT_STAGE_TBL} ;
-- END DAILY LDJSON OUTPUT FOR NOM CLIENT
-- -----------------------------------------------------------------------------
-- -----------------------------------------------------------------------------

-- -----------------------------------------------------------------------------
-- -----------------------------------------------------------------------------
-- BEGIN staging and calculations for bounce / session maps
--------------------------------------------------------------------------------
-- SESSION_EVENTCOUNT_MAP_MONTHLY_STAGE table stages the data necessary to 
-- rollup sessions into a single row of data for paralell processing by
-- the bounce and usage queries.

SET SESSION_EVENTCOUNT_MAP_MONTHLY_STAGE=session_event_counts_by_month_stage ;
-- ENSURE TABLE DOES NOT EXIST
DROP TABLE IF EXISTS ${hiveconf:SESSION_EVENTCOUNT_MAP_MONTHLY_STAGE} ;
-- CREATE TABLE
CREATE TABLE ${hiveconf:SESSION_EVENTCOUNT_MAP_MONTHLY_STAGE} (
 siteId STRING,
 userId STRING,
 sessionId STRING,
 eventName STRING,
 locations ARRAY<STRING>,
 pseudoEvents ARRAY<STRING>,
 uaGroups ARRAY<STRING>
) PARTITIONED BY (year INT ,month INT) STORED AS ORC ;
-- LOAD DATA
INSERT OVERWRITE TABLE ${hiveconf:SESSION_EVENTCOUNT_MAP_MONTHLY_STAGE}
PARTITION (year,month)
SELECT
 siteId,
 userId,
 sessionId,
 name AS eventName,
 meta.locations AS locations,
 meta.events AS pseudoEvents,
 meta.ua.ids AS uaGroups,
 year(FROM_UNIXTIME(CEIL(serverTs/1000))) AS year,
 month(FROM_UNIXTIME(CEIL(serverTs/1000))) AS month
FROM ${hiveconf:OCLN_JSON_TBL_NAME}  ;
-- GET STATS FOR TEZ
ANALYZE TABLE ${hiveconf:SESSION_EVENTCOUNT_MAP_MONTHLY_STAGE} PARTITION (year,month) COMPUTE STATISTICS;
ANALYZE TABLE ${hiveconf:SESSION_EVENTCOUNT_MAP_MONTHLY_STAGE} PARTITION (year,month) COMPUTE STATISTICS  FOR COLUMNS siteId,userId,sessionId,eventName;


SET SESSION_EVENTCOUNT_MAP_MONTHLY=session_event_counts_by_month ;
DROP TABLE IF EXISTS ${hiveconf:SESSION_EVENTCOUNT_MAP_MONTHLY};
CREATE TABLE ${hiveconf:SESSION_EVENTCOUNT_MAP_MONTHLY} STORED AS ORC AS
SELECT year, month, siteId,userId,b.sessionId, BH_union(eventMap) as eventCountMap
FROM (
    SELECT year, month,siteId,userId, a.sessionId, map(a.eventName,a.cnt) as eventMap
    FROM ( 
         SELECT year,month,
                siteId,
                userId,
                sessionId,
                eventName, 
                count(1)  as cnt
         FROM ${hiveconf:SESSION_EVENTCOUNT_MAP_MONTHLY_STAGE}
         GROUP BY
                year,month,
                siteId,
                userId,
                sessionId,
                eventName
         ORDER BY year,month,siteId,userId,sessionId,cnt DESC
    )a 
)b GROUP BY year, month,siteId,userId,b.sessionId;
--------------------------------------------------------------------------------
-- SESSION_BOUNCE_AND_USER_MONTHLY 
-- This table shows users and sessions that are sitecues users / bounc / non-bounce
-- SET TABLE NAME IN VAR
SET SESSION_BOUNCE_AND_USER_MONTHLY=session_bounce_and_sitecues_user_monthly ;
-- ENSURE TABLE DOES NOT EXIST
DROP TABLE IF EXISTS ${hiveconf:SESSION_BOUNCE_AND_USER_MONTHLY} ;
-- CREATE TABLE
CREATE TABLE ${hiveconf:SESSION_BOUNCE_AND_USER_MONTHLY} (
 siteid STRING,
 userId STRING,
 sessionid STRING,
 originaMap MAP<STRING,BIGINT>,
 filteredMap MAP<STRING,BIGINT>,
 isBounceSession BOOLEAN,
 usageEventMap MAP<STRING,BIGINT>,
 isSitecuesUser BOOLEAN,
 isZoomUser BOOLEAN,
 isTTSUser BOOLEAN,
 isHoverer BOOLEAN,
 hasIdProblems BOOLEAN
) PARTITIONED BY (year INT ,month INT) STORED AS ORC ;
--------------------------------------------------------------------------------
-- LOAD DATA
INSERT OVERWRITE TABLE ${hiveconf:SESSION_BOUNCE_AND_USER_MONTHLY}
PARTITION (year,month)
SELECT
 siteId,
 userId,
 sessionid,
 originaMap ,
 filteredMap,
 isBounceSession,
 usageEventMap,
 (usageEventMap IS NOT NULL AND SIZE(usageEventMap) >= 1) AS isSitecuesUser,
 (COALESCE(filteredMap["zoom-changed"],0) > 0) AS isZoomUser,
 (COALESCE(filteredMap["tts-requested"],0) > 0) AS isTTSUser,
 (COALESCE(filteredMap["badge-hovered"],0) > 0) AS isHoverer,
 hasIdProblems,
 year,month
FROM (
 SELECT 
  year,month,
  siteId,
  userId,
  sessionid,
  originaMap ,
  filteredMap,
  isBounceSession,
  -- filter the filteredMap so the result only contains events that are 
  -- indicative of a siteceus user.
  keepKeys(filteredMap,array('zoom-changed', 'tts-requested', 'slider-setting-changed', 'hlb-opened')) AS usageEventMap,
  hasIdProblems
 FROM (
  -- Determine bounce
  SELECT 
   year,month,
   siteId,
   userId,
   sessionid,
   originaMap ,
   filteredMap,
   -- IF the filtered Map is 1 size and contains a single page visit it is a bounceSession.
   (SIZE(filteredMap)=1 AND filteredMap['page-visited'] = 1) AS isBounceSession,
   (sessionId IS NULL OR userId IS NULL OR siteId IS NULL) hasIdProblems
  FROM (
   -- FILTER OUT pageView only sessions and sessions with only one page view to 
   -- reduce data set size.
   SELECT 
    year,month,
    siteId,
    userId,
    sessionid,
    eventCountMap AS originaMap,
    dropKeys(eventCountMap,array('error','mouse-shake')) AS filteredMap
   FROM ${hiveconf:SESSION_EVENTCOUNT_MAP_MONTHLY}
  ) AS tbl    
 ) AS TBL2 
) AS TBL3;


SET MONTHLY_SESSION_ROLLUP_TABLE=user_session_monthly_per_site;

DROP TABLE IF EXISTS ${hiveconf:MONTHLY_SESSION_ROLLUP_TABLE} ;

CREATE TABLE ${hiveconf:MONTHLY_SESSION_ROLLUP_TABLE} (
 siteId STRING,
 totalUniqueUsers BIGINT,
 totalUniqueSessions BIGINT,
 uniqueBounceUsers BIGINT,
 uniqueBounceSessions BIGINT,
 uniqueSitecuesUsers BIGINT,
 uniqueSiteceusSessions BIGINT,
 uniqueZoomUsers BIGINT,
 uniqueZoomSessions BIGINT,
 uniqueTTSUsers BIGINT,
 uniqueTTSSessions BIGINT,
 uniqueHoverUsers BIGINT,
 uniqueHoverSessions BIGINT
) PARTITIONED BY (year INT ,month INT) STORED AS ORC ;


INSERT OVERWRITE ${hiveconf:MONTHLY_SESSION_ROLLUP_TABLE} 
PARTITION (year,month)

SELECT 
 siteId, 
 totalUniqueUsers,
 totalUniqueSessions,
 uniqueBounceUsers,
 uniqueBounceSessions,
 uniqueSitecuesUsers,
 uniqueSiteceusSessions,
 uniqueZoomUsers,
 uniqueZoomSessions,
 uniqueTTSUsers,
 uniqueTTSSessions,
 uniqueHoverUsers,
 uniqueHoverSessions,
 year,
 month
FROM (
SELECT
 keyTable.siteId AS siteId, 
 keyTable.uniqueUsers AS totalUniqueUsers,
 keyTable.uniqueSessions AS totalUniqueSessions,
 COALESCE(bounceTable.uniqueUsers,0) AS uniqueBounceUsers,
 COALESCE(bounceTable.uniqueSessions,0) AS uniqueBounceSessions,
 COALESCE(sitecuesUserTable.uniqueUsers,0) AS uniqueSitecuesUsers,
 COALESCE(sitecuesUserTable.uniqueSessions,0) AS uniqueSiteceusSessions,
 COALESCE(zoomUserTable.uniqueUsers,0) AS uniqueZoomUsers,
 COALESCE(zoomUserTable.uniqueSessions,0) AS uniqueZoomSessions,
 COALESCE(ttsUserTable.uniqueUsers,0) AS uniqueTTSUsers,
 COALESCE(ttsUserTable.uniqueSessions,0) AS uniqueTTSSessions,
 COALESCE(hoverUserTable.uniqueUsers,0) AS uniqueHoverUsers,
 COALESCE(hoverUserTable.uniqueSessions,0) AS uniqueHoverSessions,
 keyTable.year AS year,
 keyTable.month AS month
FROM (
 -- Create a key table to hang the rest of the data off of.
 SELECT 
  year,
  month,siteId,
  COUNT(DISTINCT(userId)) AS uniqueUsers,
  COUNT(DISTINCT(sessionid)) AS uniqueSessions
  FROM ${hiveconf:SESSION_BOUNCE_AND_USER_MONTHLY} GROUP BY year, month, siteId 
) AS keyTable
LEFT OUTER JOIN (
 -- get bounce session data
 SELECT 
  year, month,siteId,
  COUNT(DISTINCT(userId)) AS uniqueUsers,
  COUNT(DISTINCT(sessionid)) AS uniqueSessions
 FROM ${hiveconf:SESSION_BOUNCE_AND_USER_MONTHLY} 
 WHERE isBounceSession = TRUE GROUP BY year, month, siteId 
) AS bounceTable ON keyTable.year = bounceTable.year AND keyTable.month = bounceTable.month AND keyTable.siteId = bounceTable.siteId
LEFT OUTER JOIN (
 -- get sitecues user data
 SELECT 
  year, month, 
  siteId,
  COUNT(DISTINCT(userId)) AS uniqueUsers,
  COUNT(DISTINCT(sessionid)) AS uniqueSessions
 FROM ${hiveconf:SESSION_BOUNCE_AND_USER_MONTHLY} 
 WHERE isSitecuesUser = TRUE GROUP BY year, month, siteId 
) AS sitecuesUserTable ON keyTable.year = sitecuesUserTable.year AND keyTable.month = sitecuesUserTable.month AND keyTable.siteId = sitecuesUserTable.siteId
LEFT OUTER JOIN (
 -- get sitecues zoom user data
 SELECT 
  year, month, 
  siteId,
  COUNT(DISTINCT(userId)) AS uniqueUsers,
  COUNT(DISTINCT(sessionid)) AS uniqueSessions
 FROM ${hiveconf:SESSION_BOUNCE_AND_USER_MONTHLY} 
 WHERE isZoomUser = TRUE GROUP BY year, month, siteId 
) AS zoomUserTable ON keyTable.year = zoomUserTable.year AND keyTable.month = zoomUserTable.month AND keyTable.siteId = zoomUserTable.siteId
LEFT OUTER JOIN (
 -- get sitecues TTS user data
 SELECT 
  year, month, 
  siteId,
  COUNT(DISTINCT(userId)) AS uniqueUsers,
  COUNT(DISTINCT(sessionid)) AS uniqueSessions
 FROM ${hiveconf:SESSION_BOUNCE_AND_USER_MONTHLY} WHERE isTTSUser = TRUE
 GROUP BY year, month, siteId 
) AS ttsUserTable ON keyTable.year = ttsUserTable.year AND keyTable.month = ttsUserTable.month AND keyTable.siteId = ttsUserTable.siteId
LEFT OUTER JOIN (
 -- get sitecues TTS user data
 SELECT 
  year, month, 
  siteId,
  COUNT(DISTINCT(userId)) AS uniqueUsers,
  COUNT(DISTINCT(sessionid)) AS uniqueSessions
 FROM ${hiveconf:SESSION_BOUNCE_AND_USER_MONTHLY} WHERE isHoverer = TRUE
 GROUP BY year, month, siteId 
) AS hoverUserTable ON keyTable.year = hoverUserTable.year AND keyTable.month = hoverUserTable.month AND keyTable.siteId = hoverUserTable.siteId 
) writeTbl
;

-- SELECT * FROM ${hiveconf:SESSION_BOUNCE_AND_USER_MONTHLY} WHERE isBounceSession AND (isSitecuesUser OR isZoomUser OR isTTSUser OR  isHoverer) ;

