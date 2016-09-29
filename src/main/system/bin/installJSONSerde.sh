#!/bin/bash --
################################################################################
### Resolves the directory this script is in. Tolerates symlinks.
#SOURCE="${BASH_SOURCE[0]}" ;
#while [[ -h "$SOURCE" ]] ; do # resolve $SOURCE until the file is no longer a symlink
#  TARGET="$(readlink "${SOURCE}")"
#  if [[ $SOURCE == /* ]]; then
#    #echo "SOURCE '$SOURCE' is an absolute symlink to '$TARGET'"
#    SOURCE="${TARGET}"
#  else
#   DIR="$( dirname "${SOURCE}" )"
#    #echo "SOURCE '$SOURCE' is a relative symlink to '$TARGET' (relative to '$DIR')"
#    SOURCE="${DIR}/${TARGET}" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
#  fi
#done
################################################################################
## Resolves the parent directory for this script.

#BASEDIR="$( cd -P "$( dirname "${SOURCE}" )" && pwd )" ;

LOG_FILE="~/TEST.log";
echo "USER=${USER}" > ${LOG_FILE} ; 

#sudo hadoop fs -copyToLocal s3://prd.emr.sitecues.com/serde/json-serde-1.3.7-jar-with-dependencies.jar /usr/lib/hadoop/lib/


#sudo '/usr/share/aws/emr/scripts/s3get --src=s3://prd.emr.sitecues.com/serde/json-serde-1.3.7-jar-with-dependencies.jar --dst=/usr/lib/hive/lib/'



