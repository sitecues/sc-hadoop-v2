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

LOCAL_HIVE_BASE="${PROJECT_HOME}/src/main/hive";

cat "${LOCAL_HIVE_BASE}/stdHeader.sql" ;
echo '' ;
#cat "${LOCAL_HIVE_BASE}/setupMasterInputTables.sql" ;
echo '' ;
cat "${LOCAL_HIVE_BASE}/setupNomInputTables.sql" ;
