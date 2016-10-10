-- -----------------------------------------------------------------------------
-- -----------------------------------------------------------------------------
-- BEGIN OCNL_EXPLODE_STAGE_TBL
-- Pull all fields from raw serde JSON into ORC for effiecient vectorized explosion.
-- SET TABLE NAME AS VAR
SET OCNL_EXPLODE_STAGE_TBL_NAME=clean_nom_explosion_map_stage ;
-- ENSURE TABLE DOES NOT ALREADY EXIST
DROP TABLE IF EXISTS ${hiveconf:OCNL_EXPLODE_STAGE_TBL_NAME} ;
-- CREATE TABLE
CREATE TABLE ${hiveconf:OCNL_EXPLODE_STAGE_TBL_NAME} (
 day INT,
 serverTs BIGINT,
 dateIndex INT,
 siteid STRING, 
 userId STRING,
 sessionId STRING,
 pageViewId STRING,
 eventName STRING,
 locations ARRAY<STRING>,
 pseudoEvents ARRAY<STRING>,
 uaGroups ARRAY<STRING>
) PARTITIONED BY (year INT ,month INT) STORED AS ORC ;
-- LOAD DATA INTO ${hiveconf:OCNL_EXPLODE_STAGE_TBL_NAME}
INSERT OVERWRITE TABLE ${hiveconf:OCNL_EXPLODE_STAGE_TBL_NAME}
PARTITION (year,month)
SELECT 
 day(FROM_UNIXTIME(CEIL(serverTs/1000))) AS day,
 serverTs,
 dateIndex,
 siteid, 
 userId,
 sessionId,
 pageViewId,
 name AS eventName,
 meta.locations AS locations,
 meta.events AS pseudoEvents,
 meta.ua.ids AS uaGroups,
 year(FROM_UNIXTIME(CEIL(serverTs/1000))) AS year,
 month(FROM_UNIXTIME(CEIL(serverTs/1000))) AS month
FROM ${hiveconf:OCLN_JSON_TBL_NAME} ;
-- GATHER TEX STATS FOR QUERY PLANNER.
ANALYZE TABLE ${hiveconf:OCNL_EXPLODE_STAGE_TBL_NAME} PARTITION (year,month) COMPUTE STATISTICS;
ANALYZE TABLE ${hiveconf:OCNL_EXPLODE_STAGE_TBL_NAME} PARTITION (year,month) COMPUTE STATISTICS for columns day,dateIndex,siteId,userId,sessionId,pageViewId,eventName;
-- LOG SELECT
SELECT 'COMPLETED POPULATION OF ${hiveconf:OCNL_EXPLODE_STAGE_TBL_NAME}';
-- END OCNL_EXPLODE_STAGE_TBL
-- -----------------------------------------------------------------------------
-- -----------------------------------------------------------------------------

-- -----------------------------------------------------------------------------
-- -----------------------------------------------------------------------------
-- BEGIN OCNL_EXPLODE_TBL
-- An example of explosion for ua.all groups and sudo events.
-- SET TABLE NAME AS VAR
SET OCNL_EXPLODE_TBL_NAME=clean_nom_explosion_map ;
-- ENSURE TABLE DOES NOT EXIST
DROP TABLE IF EXISTS ${hiveconf:OCNL_EXPLODE_TBL_NAME} ;
-- CREATE TABLE
CREATE TABLE ${hiveconf:OCNL_EXPLODE_TBL_NAME} (
 serverTs BIGINT,
 siteid STRING, 
 userId STRING,
 sessionId STRING,
 pageViewId STRING,
 eventName STRING,
 location STRING,
 pseudoEvent STRING,
 uaGroup STRING
) PARTITIONED BY (year INT,month INT,day INT,dateIndex int) STORED AS ORC ;
-- LOAD DATA INTO ${hiveconf:OCNL_EXPLODE_TBL_NAME}
INSERT OVERWRITE TABLE ${hiveconf:OCNL_EXPLODE_TBL_NAME} 
PARTITION (year,month,day,dateIndex)
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
 year,
 month,
 day,
 dateIndex
FROM ${hiveconf:OCNL_EXPLODE_STAGE_TBL_NAME} LATERAL VIEW explode(locations) adTable AS location 
LATERAL VIEW explode(pseudoEvents) abTable AS pseudoEvent 
LATERAL VIEW explode(uaGroups) acTable AS uaGroup ;
-- GATHER TEZ STATS FOR QUERY PLANNER.
ANALYZE TABLE ${hiveconf:OCNL_EXPLODE_TBL_NAME} PARTITION (year,month,day,dateIndex) COMPUTE STATISTICS;
ANALYZE TABLE ${hiveconf:OCNL_EXPLODE_TBL_NAME} PARTITION (year,month,day,dateIndex) COMPUTE STATISTICS for columns;
-- LOG SELECT
SELECT 'COMPLETED POPULATION OF ${hiveconf:OCNL_EXPLODE_TBL_NAME}';


-- -----------------------------------------------------------------------------
-- -----------------------------------------------------------------------------
-- BEGIN DAILY_SESSION_ROLLUP
-- Reduce ${hiveconf:OCNL_EXPLODE_TBL_NAME} to a daily session report.
-- This rollup creates a daily count of unique users,session, pageviews and 
-- event an event is a permutation of location,pseudoEvent,uaGroup.
-- TODO: Tune so we do not have to manually restrict the number of reducers.
-- This will probubly have to be rewritten using group by instead of count(Distinct))
-- SET TABLE NAME AS VAR
SET DAILY_SESSION_ROLLUP_TBL_NAME=daily_session_rollup ;
-- ENSURE TABLE DOES NOT ALREADY EXIST
DROP TABLE IF EXISTS ${hiveconf:DAILY_SESSION_ROLLUP_TBL_NAME} ;
-- CREATE TABLE / LOAD DATA
CREATE TABLE ${hiveconf:DAILY_SESSION_ROLLUP_TBL_NAME} STORED AS ORC AS
SELECT 
 year,
 month,
 day,
 dateIndex,
 COUNT(DISTINCT(userId)) as uniqueUserIds,
 COUNT(DISTINCT(sessionId)) as uniqueSessionIds,
 COUNT(DISTINCT(pageViewId)) as uniquePageViewIds,
 location,
 pseudoEvent,
 uaGroup,
 COUNT(1) AS totalOccurances
 FROM ${hiveconf:OCNL_EXPLODE_TBL_NAME} GROUP BY year,month,day,dateIndex,location,pseudoEvent,uaGroup ;
-- GATHER TEX STATS FOR QUERY PLANNER.
ANALYZE TABLE ${hiveconf:DAILY_SESSION_ROLLUP_TBL_NAME} COMPUTE STATISTICS;
ANALYZE TABLE ${hiveconf:DAILY_SESSION_ROLLUP_TBL_NAME} COMPUTE STATISTICS for columns;
-- END DAILY_SESSION_ROLLUP
-- -----------------------------------------------------------------------------
-- -----------------------------------------------------------------------------



-- -----------------------------------------------------------------------------
-- -----------------------------------------------------------------------------
-- BEGIN MONTHLY_SESSION_ROLLUP_TBL
-- SET TABLE NAME IN VAR
SET MONTHLY_SESSION_ROLLUP_TBL_NAME=monthly_session_rollup ;
-- ENSURE TABLE DOES NOT ALREADY EXIST
DROP TABLE IF EXISTS ${hiveconf:MONTHLY_SESSION_ROLLUP_TBL_NAME} ;
-- CREATE TABLE / LOAD DATA
CREATE TABLE ${hiveconf:MONTHLY_SESSION_ROLLUP_TBL_NAME} STORED AS ORC AS
SELECT 
 year,
 month,
 COUNT(DISTINCT(day)) AS numDays,
 siteId,
 COUNT(DISTINCT(userId)) as uniqueUserIds,
 COUNT(DISTINCT(sessionId)) as uniqueSessionIds,
 COUNT(DISTINCT(pageViewId)) as uniquePageViewIds,
 location,
 pseudoEvent,
 uaGroup,
 COUNT(1) AS totalOccurances
FROM ${hiveconf:OCNL_EXPLODE_TBL_NAME} GROUP BY year,month,siteId,location,pseudoEvent,uaGroup ;
-- GATHER STATS FOR TEZ
ANALYZE TABLE ${hiveconf:MONTHLY_SESSION_ROLLUP_TBL_NAME} COMPUTE STATISTICS;
ANALYZE TABLE ${hiveconf:MONTHLY_SESSION_ROLLUP_TBL_NAME} COMPUTE STATISTICS for columns;
-- END MONTHLY_SESSION_ROLLUP_TBL
-- -----------------------------------------------------------------------------
-- -----------------------------------------------------------------------------
