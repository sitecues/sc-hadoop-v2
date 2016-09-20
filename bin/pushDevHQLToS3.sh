#!/bin/bash --
################################################################################
## Resolves the directory this script is in. Tolerates symlinks.
SOURCE="${BASH_SOURCE[0]}" ;
while [[ -h "$SOURCE" ]] ; do # resolve $SOURCE until the file is no longer a symlink
  TARGET="$(readlink "${SOURCE}")"
  if [[ $SOURCE == /* ]]; then
    #echo "SOURCE '$SOURCE' is an absolute symlink to '$TARGET'"
    SOURCE="${TARGET}"
  else
    DIR="$( dirname "${SOURCE}" )"
    #echo "SOURCE '$SOURCE' is a relative symlink to '$TARGET' (relative to '$DIR')"
    SOURCE="${DIR}/${TARGET}" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
  fi
done
################################################################################
## Resolves the parent directory for this script.

BASEDIR="$( cd -P "$( dirname "${SOURCE}" )" && pwd )" ;
PROJECT_HOME=`pushd "${BASEDIR}/../" > /dev/null ; pwd ;popd > /dev/null` ;
BIN_DIR="${PROJECT_HOME}/bin" ;

################################################################################
## Import Functions
source ${BASEDIR}/Functions.sh

S3_BUCKET="s3://dev.emr.sitecues.com" ;
LOCAL_HIVE_BASE="${BASEDIR}/src/main/hive/";
LOCAL_JOB_FILE="${LOCAL_HIVE_BASE}/devJob.hql" ;



function getDateForFileName(){
 date -u +"%Y-%m-%d-%H.%M.%S"
}


JOB_NAME="$(getDateForFileName).devSCHiveJob.hql"

LOCAL_TMP_JOB_FILE="/tmp/${JOB_NAME}";
echo '' > ${LOCAL_TMP_JOB_FILE} ;

pushd ${BIN_DIR} > /dev/null ;
 ./makeDevHiveSetupJob.sh > ${LOCAL_TMP_JOB_FILE};
popd > /dev/null ;

PUSH_TO_DIR="${S3_BUCKET}/tmp/jobs/sc" ;
s3cmd put --no-progress "${LOCAL_TMP_JOB_FILE}" "${PUSH_TO_DIR}/" &> /dev/null ;
echo "${PUSH_TO_DIR}/${JOB_NAME}" ;










