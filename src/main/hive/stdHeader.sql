-- Put at the top of every job
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveCombineSplitsInputFormat;
SET mapred.min.split.size=200000000;
SET hive.merge.mapfiles=true ;
SET hive.merge.smallfiles.avgsize=1000000000;
SET hive.exec.compress.output=true;
SET mapred.compress.map.output=true ;
SET mapred.map.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapred.output.compression.type=BLOCK;
ADD JAR s3://prd.emr.sitecues.com/serde/json-serde-1.3.7-jar-with-dependencies.jar ;
-- END STANDARD HEADER FOR CONFIG ON HIVE EMR
