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



