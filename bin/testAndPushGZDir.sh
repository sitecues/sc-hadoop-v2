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
die () {
    echo -e >&2 "$@"
    exit 1
}

[ "$#" -eq 2 ] || die "2 arguments required, $# provided. \nUSAGE:: ${SOURCE} \${SOURCE_DIR} \${S3_DEST_DIR}" ;

SOURCE_DIR=`cd "${1}"; pwd` ;
S3_DEST_DIR="${2}" ;

GLOBAL_TIMER=timer;
printHeader ;
printMSG "Checking for non-tmp gz files in local directory ${SOURCE_DIR}" ;

WROTE_TMP=1;

#Ensure the tmp file is removed
trap "echo 'cleaning up ${SOURCE_DIR}' ; if [[ ${WROTE_TMP} = 0 ]]; then rm ${SOURCE_DIR}/working.txt ; fi" INT

function waitIfFileExists() {
 local WORKING_FILE="${1}"
 local SLEEP_SECONDS="${2}"
 if [[ -f working.txt ]] ;  then
  printMSG "The lock file ${WORKING_FILE} exists. The directory at ${SOURCE_DIR} is being worked on by another process. Waiting ${SLEEP_SECONDS} seconds and trying again" ;
  sleep ${SLEEP_SECONDS}s ;
  waitIfFileExists "${1}";
 fi;
}

pushd ${SOURCE_DIR} > /dev/null ;
 waitIfFileExists "${SOURCE_DIR}/working.txt" 10 ;
 echo "${SOURCE}" >> "${SOURCE_DIR}/working.txt" ;
 WROTE_TMP=0;
ls
 ls *.gz | grep -v tmp | while read line ; do 
    gzip -t ${line} ; 
    if [[ ${?} = 0 ]] ; then 
     printMSG "Tested ${line} Result IS VALID GZ. Continuing with s3 sync."
     s3cmd --verbose sync "${SOURCE_DIR}/${line}" "${S3_DEST_DIR}/" ;
     if [[ ${?} = 0 ]]; then 
      printMSG "Successfully synced ${SOURCE_DIR}/${line} to ${S3_DEST_DIR}/ Continuing."
     else
      printMSG "ERROR syncing ${SOURCE_DIR}/${line} to ${S3_DEST_DIR}/ FAILING."
      rm "${SOURCE_DIR}/working.txt" ;
      popd > /dev/null;
      printFooter ;
      exit 1;
     fi
    else
     printMSG "Tested ${line} Result IS NOT VALID GZ. Aborting transfer."
    fi
 done ; 
 rm "${SOURCE_DIR}/working.txt" ;
popd > /dev/null ;

printFooter ;
