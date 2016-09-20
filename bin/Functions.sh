#!/bin/bash --
################################################################################
#
#   FILE:   FunctionBase.sh
#
#   Usage:  source FunctionBase.sh
#
#   DESCRIPTION: Repository for common functions
#
#   OPTIONS: ---.
#   REQUIREMENTS: ---
#   BUGS: ---
#   NOTES: ---
#   AUTHOR: Brian M. Lima
#   VERSION: 0.9
#   CREATED: 2010-12-24 08:21
################################################################################

################################################################################
# FUNCTIONS
################################################################################

################################################################################
#Basic Timer Function for global and single commands. Real elapsed time only.
function timer() {
    if [[ $# -eq 0 ]] ; then
        echo $(date '+%s') ;
    else
        local  stime=$1 ;
        etime=$(date '+%s') ;
        if [[ -z "${stime}" ]]; then stime=$etime; fi ;
        dt=$((etime - stime)) ;
        ds=$((dt % 60)) ;
        dm=$(((dt / 60) % 60)) ;
        dh=$((dt / 3600)) ;
        printf '%d:%02d:%02d' $dh $dm $ds ;
    fi ;
} ;
################################################################################

################################################################################
# BML: Simple fork helper method. Call consecutively to run multiple parallel
# jobs. This is easier than using counters and allows you to keep a stack of
# processes running constantly as opposed to running X processes in parallel and
# waiting for them all to finish using wait before starting another X parallel
# processes. NOTE: use wait after your execution loop to allow any background
# processes to finish before moving on.
function fork() {
 local num_par_procs ;
 if [[ -z $1 ]] ; then num_par_procs=3 ; else num_par_procs=$1 ; fi ;
 while [[ $(jobs | wc -l) -ge $num_par_procs ]] ; do sleep 1 ; done ;
}
################################################################################

################################################################################
# BML: Simple helper function for creating dates formatted YYYY-MM-DD.
# PARAMETERS:
#  Pass the number of days in the past you want a date for.
# NOTE: This function uses the core date utility so it will handle leap years
#  future Julian date advances and logical month boundaries. Check the system
#  clock first if this function returns odd dates.
################################################################################
function makeDate() {
if [ ${#} -eq 0 ] ; then
  local DATE_STR="0 days ago" ;
 else
  local DATE_STR="${1} days ago" ;
 fi ;
 ###############################################################################
 local YEAR=`date --date="${DATE_STR}" +%Y` ;
 local MONTH=`date --date="${DATE_STR}" +%m` ;
 local DAY=`date --date="${DATE_STR}" +%d` ;
 echo "${YEAR}-${MONTH}-${DAY}" ;
}
################################################################################

################################################################################
function printBreak(){
 echo '################################################################################' ;
};
################################################################################



################################################################################
function printMSG(){
 #printBreak ;
 echo "# ${1}" ;
 #printBreak ;
};
################################################################################


################################################################################
function printHeader() {
 printBreak ;
 printMSG "$(basename $0) BEGIN AT $(date)" ;
 printBreak ;
 echo '' ;
};
################################################################################

################################################################################
function printFooter(){
 echo '' ;
 printBreak ; 
 printMSG  "Elapsed time: $(timer ${GLOBAL_TIMER})" ;
 printMSG "$(basename $0) END AT $(date)" ;
 printBreak ;
};
################################################################################



################################################################################
function checkSudo(){
 sudo -n echo '0' &> /dev/null ;
 if [[ ${?} = 0 ]] ; then
  printMSG "SUCCESS: USER ${USER} has verified sudo privilages.";
  return 0;
 else
  printMSG "WARN: USER ${USER} does not have sudo privilages.";
  return 1;
 fi;
}
################################################################################

################################################################################
## Simple check for a return status, logging, and exit on failure.
function checkStatus() {
 local STATUS=${1} ;
 local OK_MESSAGE="$2" ;
 FAIL_MESSAGE="$3" ;
 if [[ ${STATUS} == 0 ]] ; then
  printMSG "${OK_MESSAGE}" ;
 else
  printMSG "${FAIL_MESSAGE}" ;
  exit ${STATUS} ;
 fi ;
}

################################################################################
## Removes the directory passed in the first argument and clones the git 
## repository passed in the second. Fails on anything but success.
function removeAndCloneRepo() {
 local EXISTING_DIR="${1}" ;
 local GIT_REPO_PATH="${2}" ;
 ##Remove existing code base.
 printMSG "Attempting Removal of Code Base Directory At ${EXISTING_DIR}" ;
 rm -rf "${EXISTING_DIR}" ;
 checkStatus ${?} "Removed Directory At ${EXISTING_DIR}" "Unable to Remove Directory at ${EXISTING_DIR}" ;
 ##Clone fresh repo.
 printMSG "Attempting Clone of Repository At ${GIT_REPO_PATH}" ;
 git clone "${GIT_REPO_PATH}" ;
 checkStatus ${?} "Cloned Repository At ${GIT_REPO_PATH}" "Unable to Clone Repository At ${GIT_REPO_PATH}" ;
}