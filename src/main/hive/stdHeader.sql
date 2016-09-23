-- Put at the top of every job

-- Can combine gzips
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveCombineSplitsInputFormat;

-- Merge intermediate map results, otherwise you'll have as many reducers as mappers (way more efficient)
SET hive.merge.mapfiles=true ;

-- Size config
SET hive.merge.smallfiles.avgsize=1000000000;
SET mapred.min.split.size=200000000;

-- Outut and intermediate output compression
SET hive.exec.compress.output=true;
SET mapred.compress.map.output=true ;

-- Best compression codec for medium-sized text data
SET mapred.map.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapred.output.compression.type=BLOCK;

-- Allow parsing of JSON into data columns
ADD JAR s3://prd.emr.sitecues.com/serde/json-serde-1.3.7-jar-with-dependencies.jar ;

-- END STANDARD HEADER FOR CONFIG ON HIVE EMR
