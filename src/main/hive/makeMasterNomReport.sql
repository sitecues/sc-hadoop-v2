-- Brian's scratchpad --

SELECT 
raw_master_log_meta.filename , 
raw_master_log_meta.linecount,
clean_nom_log_meta.linecount
FROM raw_master_log_meta
LEFT outer join clean_nom_log_meta on raw_master_log_meta.filename = clean_nom_log_meta.filename;



SELECT 
 fileName,
 name,
 COUNT(*) AS occuranceCount
FROM (
 SELECT 
  SPLIT(INPUT__FILE__NAME,'clean/')[1] AS fileName, 
  get_json_object(line, '$.name') AS name
 FROM clean_nom_log
) AS tbl group by fileName,name ORDER BY fileName ASC, occuranceCount DESC ;



SELECT get_json_object(line, '$.clientData.name') AS name,line FROM optimized_raw_master_log limit 10 ;

SELECT 
raw_master_log_meta.filename , 
raw_master_log_meta.linecount,
clean_nom_log_meta.linecount
FROM raw_master_log_meta
LEFT outer join clean_nom_log_meta on raw_master_log_meta.filename = clean_nom_log_meta.filename;


--  SELECT * from is_optimized_raw_master_log_consistent ;
--  SELECT fileName FROM optimized_clean_nom_log ;

SELECT
 tbl1.fileName,
 COALESCE(raw_master_log_meta.lineCount,0) AS masterEventCount,
 COALESCE(clean_nom_log_meta.lineCount,0) AS nomEventCount
FROM 
(
 SELECT fileName FROM raw_master_log_meta group by filename
 UNION ALL
 SELECT fileName FROM clean_nom_log_meta group by filename 
) AS tbl1 
LEFT OUTER JOIN raw_master_log_meta ON tbl1.fileName = raw_master_log_meta.fileName
LEFT OUTER JOIN clean_nom_log_meta ON tbl1.fileName = clean_nom_log_meta.fileName
;

SELECT
 tbl1.fileName,
 COALESCE(raw_master_log_meta.lineCount,0) AS masterEventCount,
 COALESCE(clean_nom_log_meta.lineCount,0) AS nomEventCount
FROM 
(
 SELECT fileName FROM raw_master_log_meta group by filename
 UNION ALL
 SELECT fileName FROM clean_nom_log_meta group by filename 
) AS tbl1 
LEFT OUTER JOIN raw_master_log_meta ON tbl1.fileName = raw_master_log_meta.fileName
LEFT OUTER JOIN clean_nom_log_meta ON tbl1.fileName = clean_nom_log_meta.fileName
;

SELECT filename, get_json_object(line, '$.siteId') AS siteId FROM optimized_clean_nom_log limit 10 ;
