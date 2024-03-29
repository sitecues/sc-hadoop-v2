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


function getDateForFileName(){
 date -u +"%Y-%m-%d-%H.%M.%S"
}

CLUSTER_TAGS='STATE=DISPOSABLE' ;
EC2_ATTRIBITES='{"KeyName":"BrianMLima","InstanceProfile":"EMR_EC2_DefaultRole","AvailabilityZone":"us-east-1b","EmrManagedSlaveSecurityGroup":"sg-b9ede2d3","EmrManagedMasterSecurityGroup":"sg-95ece3ff"}' ;

SERVICE_ROLE="EMR_DefaultRole" ;

RELEASE_LABEL="emr-5.0.0" ;

S3N_LOG_URI='s3n://aws-logs-573289937126-us-east-1/elasticmapreduce/' ; 

INSTANCE_GROUPS='[{"InstanceCount":3,"InstanceGroupType":"CORE","InstanceType":"m3.xlarge","Name":"Core instance group - 2"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"m3.xlarge","Name":"Master instance group - 1"}]'

EC2_REGION="us-east-1"

CLUSTER_NAME="${1}" ;
HIVE_SCRIPT_NAME="${2}" ;

STEPS='[{"Args":["hive-script","--run-hive-script","--args","-f","'"${HIVE_SCRIPT_NAME}"'","--verbose","","-d","OUTPUT=s3://output"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Name":"Hive program"}]' 

printMSG "CLUSTER_NAME=${CLUSTER_NAME}"
printMSG "HIVE_SCRIPT_NAME=${HIVE_SCRIPT_NAME}"


#TERMINATE="--auto-terminate" ;
aws emr create-cluster ${TERMINATE} --applications Name=Hadoop Name=Hive Name=Pig Name=Hue --tags "${CLUSTER_TAGS}" --ec2-attributes "${EC2_ATTRIBITES}"  --service-role ${SERVICE_ROLE} --enable-debugging --release-label "${RELEASE_LABEL}" --log-uri "${S3N_LOG_URI}" --name "${CLUSTER_NAME}" --instance-groups "${INSTANCE_GROUPS}" --region ${EC2_REGION} --steps "${STEPS}"
