--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Put at the top of every job
-- BEGIN STANDARD HEADER FOR CONFIG ON HIVE EMR
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
-- END STANDARD HEADER FOR CONFIG ON HIVE EMR
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

