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


