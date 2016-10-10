-- Brian's scratch pad of ideas

DROP TABLE IF EXISTS clean_nom_log;
DROP TABLE IF EXISTS optomized_clean_nom_log;
DROP TABLE IF EXISTS optimized_clean_nom_log;
DROP TABLE IF EXISTS packed_raw_nom_log;
DROP TABLE IF EXISTS raw_metrics_log;
DROP TABLE IF EXISTS clean_nom_log_metz;
DROP TABLE IF EXISTS clean_nom_log_meta;
DROP TABLE IF EXISTS clean_nom_log_event_counts;

DROP TABLE IF EXISTS is_optimized_clean_nom_log_consistent ;
DROP TABLE IF EXISTS optimized_raw_master_log ;
DROP TABLE IF EXISTS raw_master_log ;
DROP TABLE IF EXISTS raw_master_log_meta ;


DROP TABLE IF EXISTS optimized_clean_nom_log_json;

's3://data.sitecues.com/telem/hive/clean_nom_log_json/'

DROP TABLE IF EXISTS clean_nom_log_json;
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
    name : STRING,
    pageViewId: STRING,
    serverTs: BIGINT,
    sessionId: STRING,
    siteId: STRING,
    ttsState : BOOLEAN,
    userId: STRING,
    zoomLevel: DOUBLE
) ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
location 's3://data.sitecues.com/telem/hive/clean_nom_log_json/' ;

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



SELECT eventId,counter FROM (
select evTbl.eventId, count(evTbl.eventId) as counter FROM (SELECT eventId FROM clean_nom_log_json) AS evTbl GROUP BY evTbl.eventId
) as tbl  WHERE counter = 1 LIMIT 10;

SELECT COUNT(*) FROM clean_nom_log_json ;




SELECT eventId,counter FROM (
select evTbl.eventId, count(evTbl.eventId) as counter FROM (SELECT eventId FROM clean_nom_log_json) AS evTbl GROUP BY evTbl.eventId
) as tbl  WHERE counter = 1 LIMIT 10;



SELECT siteid, adid
FROM clean_nom_log_json LATERAL VIEW explode(clean_nom_log_json.meta.locations) adTable AS adid LIMIT 10;

SET OCNL_EXPLODE_TBL_NAME=test_view_clean_nom_explosion ;

create table ${hiveconf:OCNL_EXPLODE_TBL_NAME} STORED AS ORC AS
SELECT 
 year(FROM_UNIXTIME(UNIX_TIMESTAMP(FROM_UNIXTIME(CEIL(serverTs/1000)), 'yyyy-mm-dd'))) AS year,
 month(FROM_UNIXTIME(UNIX_TIMESTAMP(FROM_UNIXTIME(CEIL(serverTs/1000)), 'yyyy-mm-dd'))) AS month,
 day(FROM_UNIXTIME(UNIX_TIMESTAMP(FROM_UNIXTIME(CEIL(serverTs/1000)), 'yyyy-mm-dd'))) AS day,
 serverTs, 
 siteid, 
 name AS eventName, 
 location,
 pseudoEvenent,
 uaGroup
FROM ${hiveconf:OCLN_JSON_TBL_NAME} LATERAL VIEW explode(meta.locations) adTable AS location 
LATERAL VIEW explode(meta.pseudoevents) abTable AS pseudoEvenent 
LATERAL VIEW explode(meta.ua.groups) acTable AS uaGroup ;



-- Extract the raw data necessary to audit ids and sessionize
CREATE TABLE clean_nom_id_audit_stage_001 STORED AS SEQUENCEFILE AS
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
 eventName
FROM (
 SELECT 
  FROM_UNIXTIME(UNIX_TIMESTAMP(FROM_UNIXTIME(CEIL(serverTs/1000)), 'yyyy-MM-dd HH:mm:ss')) AS unixTs
  siteid, 
  userId,
  sessionId,
  pageViewId
  name AS eventName
 FROM clean_nom_log_json 
)tbl;



SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveCombineSplitsInputFormat;
SET mapred.min.split.size=200000000;
SET hive.merge.mapfiles=true ;
SET hive.merge.smallfiles.avgsize=1000000000;
SET hive.exec.compress.output=true;
SET mapred.compress.map.output=true ;
SET mapred.map.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapred.output.compression.type=BLOCK;

SET SAMPLE_RATE=1;

SET hive.variable.substitute.depth=100 ;

SET OLD_HIVE_INPUT_FORMAT=${hive.input.format} ;
SET OLD_TEZ_INPUT_FORMAT=${hive.tez.input.format} ;

SET hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.tez.input.format=${hive.input.format};

SET OCNL_SOURCE_TBL=optimized_clean_nom_log_sample ;

DROP TABLE IF EXISTS ${hiveconf:OCNL_SOURCE_TBL};
CREATE TABLE ${hiveconf:OCNL_SOURCE_TBL} STORED AS SEQUENCEFILE AS
SELECT
 INPUT__FILE__NAME AS sourceFileAbsolutePath,
 SPLIT(INPUT__FILE__NAME,'clean/')[1] AS fileName,
 line
FROM clean_nom_log TABLESAMPLE(${hiveconf:SAMPLE_RATE} PERCENT) s ;

SET hive.input.format=${hiveconf:OLD_HIVE_INPUT_FORMAT};
SET hive.tez.input.format=${hiveconf:OLD_TEZ_INPUT_FORMAT};
SELECT COUNT(*) FROM ${hiveconf:OCNL_SOURCE_TBL} ;


SET CNL_SOURCE_S3_LOCATION=s3://data.sitecues.com/telem/nom/clean/;
SET CNL_SOURCE_TBL_NAME=clean_nom_log_source;
DROP TABLE IF EXISTS ${hiveconf:CNL_SOURCE_TBL_NAME};
CREATE EXTERNAL TABLE ${hiveconf:CNL_SOURCE_TBL_NAME} (line string) location '${hiveconf:CNL_SOURCE_S3_LOCATION}';


SET OCNL_SOURCE_TBL=optimized_clean_nom_log_sample ;
DROP TABLE IF EXISTS mx_000_stage ;
CREATE EXTERNAL TABLE mx_000_stage (
 line String
) STORED AS TEXTFILE
LOCATION 's3://data.sitecues.com/telem/hive/mx_000_stage' ;
INSERT OVERWRITE TABLE mx_000_stage SELECT line FROM ${hiveconf:OCNL_SOURCE_TBL} ;



ADD JAR s3://prd.emr.sitecues.com/serde/json-serde-1.3.7-jar-with-dependencies.jar ;
ADD JAR s3://prd.emr.sitecues.com/udf/json-udf-1.3.7-jar-with-dependencies.jar ;
DROP FUNCTION tjson ;
CREATE FUNCTION tjson as 'org.openx.data.udf.JsonUDF';
SELECT
tjson(
 NAMED_STRUCT(
  "day",CONCAT(year,'-',LPAD(month,2,'0'),'-',LPAD(day,2,'0')),
  "siteId", siteId,
 )
)
FROM test_view_reduced_001_explosion LIMIT 10 ;



SELECT b.sessionId, collect_set(concat_ws(':',map_keys(b.eventMap),map_values(b.eventMap))) as eventCountMap
FROM
(
    SELECT a.sessionId, map(a.eventName,a.cnt) as eventMap
    FROM
    ( 
         SELECT sessionId,
                eventName, 
                cast(count(1) as string) as cnt
         FROM test_view_clean_nom_explosion
         GROUP BY 
                sessionId,
                eventName
    )a
)b
GROUP BY b.sessionId LIMIT 100;


1454284796977

1453812096739

1454284799633



 SELECT
 serverTs,
 CEIL(serverTs/1000) AS theCeil,
 FROM_UNIXTIME(CEIL(serverTs/1000)) AS theFromUnixTime,
 UNIX_TIMESTAMP(FROM_UNIXTIME(CEIL(serverTs/1000))) AS theUnixTimestamp,
 year(FROM_UNIXTIME(UNIX_TIMESTAMP(FROM_UNIXTIME(CEIL(serverTs/1000))))) AS year,
 month(FROM_UNIXTIME(UNIX_TIMESTAMP(FROM_UNIXTIME(CEIL(serverTs/1000))))) AS month,
 day(FROM_UNIXTIME(UNIX_TIMESTAMP(FROM_UNIXTIME(CEIL(serverTs/1000))))) AS day
 FROM (
 SELECT 
  DISTINCT(serverTs) AS serverTs 
  FROM  test_view_clean_nom_explosion 
  WHERE test_view_clean_nom_explosion.month=1 AND test_view_clean_nom_explosion.day=1 ORDER BY serverTs ASC
  ) AS tbl ;



 SELECT
 serverTs,
 CEIL(serverTs/1000) AS theCeil,
 FROM_UNIXTIME(CEIL(serverTs/1000)) AS theFromUnixTime,
 UNIX_TIMESTAMP(FROM_UNIXTIME(CEIL(serverTs/1000))) AS theUnixTimestamp,
 year(FROM_UNIXTIME(CEIL(serverTs/1000))) AS year,
 month(FROM_UNIXTIME(CEIL(serverTs/1000))) AS month,
 day(FROM_UNIXTIME(CEIL(serverTs/1000))) AS day
 FROM (
 SELECT 
  DISTINCT(serverTs) AS serverTs 
  FROM  test_view_clean_nom_explosion 
  WHERE test_view_clean_nom_explosion.month=1 AND test_view_clean_nom_explosion.day=1 ORDER BY serverTs ASC
  ) AS tbl ;






RESET ;
-- Set limit of variables to 100, default is 20.
SET hive.variable.substitute.depth=100 ;
-- Allow the TEZ Vectorized query engine to run when possible on queries that 
-- use primitives only on tables that are stored in ORC format. Similar behavior
-- under the hood to a large scale columnar DB, transparent to the developer.
SET hive.vectorized.execution.reduce.enabled = true;
-- Allow Hive to combine gz files read from s3 for source table reads.
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveCombineSplitsInputFormat;
-- Set the minimum size for a read chunk
SET mapred.min.split.size=200000000;
-- Hive should attempt to merge small map files to reduce file count in 
-- intermediate and final results.
SET hive.merge.mapfiles=true ;
-- Set the average size to target for merging of small map files.
SET hive.merge.smallfiles.avgsize=1000000000;
-- Set the default for compression of output. This is true to reduce reads
SET hive.exec.compress.output=true;
-- Set the default for compression of intermediate mapper output. This is true 
-- to reduce reads and reduce overhead of data transfer between mappers and 
-- reducers.
SET mapred.compress.map.output=true ;
-- Set up compression codecs for intermediate map output.
SET mapred.map.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
-- Set job output compression codec.
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
-- Set compression size-type to HDFS block size.
SET mapred.output.compression.type=BLOCK;
-- Add the JSON serde used to read JSON structures in tables.
ADD JAR s3://prd.emr.sitecues.com/serde/json-serde-1.3.7-jar-with-dependencies.jar ;
ADD JAR s3://prd.emr.sitecues.com/udf/json-udf-1.3.7-jar-with-dependencies.jar ;
-- WELCOME TO THE BRICKHOUSE. Super handy UDF's
ADD JAR s3://prd.emr.sitecues.com/udf/brickhouse-0.7.1-SNAPSHOT.jar ;


SET OCNL_EXPLODE_TBL_NAME=test_view_clean_nom_explosion ;
set hive.execution.engine=tez;
set hive.cbo.enable=true;
set hive.compute.query.using.stats=true;
set hive.stats.fetch.column.stats=true;
set hive.stats.fetch.partition.stats=true;
set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled = true;
set hive.vectorized.execution.reduce.groupby.enabled = true;
set hive.optimize.sort.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.stats.autogather=true;
set hive.tez.auto.reducer.parallelism = true;

SET mapred.reduce.tasks = 36;

SET mapred.min.split.size=50000000;
SET hive.merge.smallfiles.avgsize=10000000;

DROP TABLE IF EXISTS test_view_reduced_001_explosion ;
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
 pseudoEvent,
 uaGroup,
 COUNT(1) AS totalOccurances
 FROM ${hiveconf:OCNL_EXPLODE_TBL_NAME} GROUP BY year,month,day,siteId,location,pseudoEvent,uaGroup ;


ANALYZE TABLE test_view_reduced_001_explosion COMPUTE STATISTICS;
 
ANALYZE TABLE test_view_reduced_001_explosion COMPUTE STATISTICS for columns;




SET mapred.reduce.tasks = 36;
SET mapred.map.tasks = 36;
DROP TABLE IF EXISTS reduced_explosion_stage_002 ;
CREATE TABLE reduced_explosion_stage_002 STORED AS ORC AS
SELECT
theDayString,
siteId,
COUNT(DISTINCT(userId)) as uniqueUserIds,
COUNT(DISTINCT(sessionId)) as uniqueSessionIds,
COUNT(DISTINCT(pageViewId)) as uniquePageViewIds,
eventPath
FROM reduced_explosion_stage_001 GROUP BY theDayString,siteId,eventPath;




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
   meta.ua.ids AS uaGroups,
   meta.events AS pseudoevents
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

DROP TABLE IF EXISTS session_event_counts_by_day ;
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
         FROM clean_nom_explosion_map
         GROUP BY
                year,month,day,
                sessionId,
                eventName
    )a
)b
GROUP BY year, month, day, b.sessionId;



ADD JAR s3://prd.emr.sitecues.com/udf/brickhouse-0.7.1-SNAPSHOT.jar ;
create temporary function collect as 'brickhouse.udf.collect.CollectUDAF';

DROP TABLE IF EXISTS session_event_counts_by_month;
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
         FROM clean_nom_explosion_map
         GROUP BY
                year,month,
                sessionId,
                eventName
    )a
)b
GROUP BY year, month, b.sessionId;











ADD JAR s3://prd.emr.sitecues.com/udf/brickhouse-0.7.1-SNAPSHOT.jar ;
create temporary function collect as 'brickhouse.udf.collect.CollectUDAF';

SET MONTHLY_SESSION_ROLLUP_OUTPUT_TBL_NAME=monthly_ses_json_output ;
DROP TABLE IF EXISTS ${hiveconf:MONTHLY_SESSION_ROLLUP_OUTPUT_TBL_NAME} ;
CREATE TABLE ${hiveconf:MONTHLY_SESSION_ROLLUP_OUTPUT_TBL_NAME} STORED AS TEXTFILE AS 
SELECT 
 named_struct(
 'dateString',dateString,
 'axis',COLLECT(aNamedStruct) 
) AS entry
FROM (
 SELECT
  CONCAT(YEAR,'-',LPAD(month,2,'0')) AS dateString,
  named_struct(
   'totalDays',numDays,
   'totalOccurances',totalOccurances,
   'uniqueUserIds',uniqueUserIds,
   'uniqueSessionIds',uniqueSessionIds,
   'uniquePageViewIds',uniquePageViewIds,
   'axisName',CONCAT(location,'||',pseudoEvent,'||',uaGroup),
   'location',location,
   'pseudoEvent',pseudoEvent,
   'uaGroup',uaGroup
 ) as aNamedStruct 
 FROM ${hiveconf:MONTHLY_SESSION_ROLLUP_TBL_NAME}
)tbl GROUP BY dateString LIMIT 10;



aws emr socks --cluster-id j-1NSCXYHZE5GSA --key-pair-file 


time  hive --verbose --hiveconf hive.root.logger=INFO,console -f tmp.txt

set hive.cli.print.header=true;






SET ALL_AXIS_JSON_DUMP_TBL=all_axis_json_dump ;
SET ALL_AXIS_JSON_DUMP_TBL_S3_LOC=s3://data.sitecues.com/telem/nom/client/axis ;
DROP TABLE IF EXISTS ${hiveconf:ALL_AXIS_JSON_DUMP_TBL} ;

CREATE EXTERNAL TABLE ${hiveconf:ALL_AXIS_JSON_DUMP_TBL} (
 collection STRING
)
STORED AS TEXTFILE 
LOCATION '${hiveconf:ALL_AXIS_JSON_DUMP_TBL_S3_LOC}' ;

create temporary function tjson as 'org.openx.data.udf.JsonUDF';
SET hive.exec.compress.output=false;
INSERT OVERWRITE TABLE ${hiveconf:ALL_AXIS_JSON_DUMP_TBL}
SELECT 
 tjson(collect(json)) AS collection
FROM (
 SELECT 
  named_struct(
   "location",location,
   "pseudoEvent",pseudoEvent,
   "uaGroup",uaGroup, 
   "totalOccurances",COUNT(1), 
   "minDateIndex",MIN(dateIndex), 
   "maxDateIndex",MAX(dateIndex)
  ) AS json
 FROM ${hiveconf:OCNL_EXPLODE_TBL_NAME} GROUP BY location,pseudoEvent,uaGroup
) tbl ;
set hive.exec.compress.output=true;




SELECT year, month,day, dateIndex, COUNT(1) FROM clean_nom_explosion_map_stage GROUP BY year,month,day,dateIndex ORDER BY year,month,day,dateIndex DESC; 




-- -----------------------------------------------------------------------------
-- -----------------------------------------------------------------------------
-- -----------------------------------------------------------------------------
-- BEGIN DAILY LDJSON OUTPUT FOR NOM CLIENT
-- The base s3 location all ldjson will be output under
SET DAILY_NOM_S3_BASE_LOC=s3://data.sitecues.com/telem/hive/output/nom/daily/ ;
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
   FROM ${hiveconf:DAILY_SESSION_ROLLUP_TBL_NAME}
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
   FROM ${hiveconf:DAILY_SESSION_ROLLUP_TBL_NAME}
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
   FROM ${hiveconf:DAILY_SESSION_ROLLUP_TBL_NAME}
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
