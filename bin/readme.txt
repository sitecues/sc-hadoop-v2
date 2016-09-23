runDev.sh
  Master script that calls:
    makeDevHiveSetupJob.sh
      Script that creates a hive job to import and process the data.
      Never run but cool because it shows actual code that's running when you use runDev.sh.
    pushDevHQLToS3.sh
      Takes output of makeDevHiveSetupJob.sh and pushes results to s3
    Functions.sh
      Library of utility functions
    runCluster.sh
      Starts physical hadoop cluster and runs the hive job

testAndPushGZDir.sh
  Gets a local directory of cleaned metrics data (.gz)
  Takes care of waiting for locks, locking, unlocking
