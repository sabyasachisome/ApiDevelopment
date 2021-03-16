#!/bin/bash

###Purpose: This script is made for the below reasons:
#1. Call the python main script(which refreshes the clean table with updated data from omdb api)
#2. Check if previous job is still running before running the main script
##Owner: Sabya
##Version:1.0
##Usage:runner.py

cur_ts=`date +%Y%m%d_%H%M%S`

##one time creation
#mkdir hollwood_case_study
#cd hollwood_case_study
#mkdir logs
#mkdir shell_scripts

#creating the path variables
logpath=${HOME}/hollwood_case_study/logs
script_path=${HOME}/hollwood_case_study/shell_scripts
logfile=${logpath}/python_job_${cur_ts}.log
touch logfile

lockfile=${logpath}/python_job.lck

echo [`date +%Y-%m-%d_%H:%M:%S`] " : Cleaning 2 day old log files" >> ${logfile}
find ${logpath} -name "*.log" -type f -mtime +2 -print -delete

#below mechanism checks if the previous job is still running
if [ -f ${lockfile} ];
then
	echo [`date +%Y-%m-%d_%H:%M:%S`] " : Previous job still running. Please kill the previous job and restart." >> ${logfile}
	exit 1
else
	echo [`date +%Y-%m-%d_%H:%M:%S`] " : Starting Python job" >> ${logfile}
fi

touch ${lockfile}

#starting the main python job
python ${script_path}/runner.py >> ${logfile} 2>&1
python_job_status=$?

if [ python_job_status -eq 0 ];
then
	echo [`date +%Y-%m-%d_%H:%M:%S`] " : Data refreshed successfully" >> ${logfile}
	rm ${lockfile}
	echo "job run successful" >> ${logfile}
else
	echo [`date +%Y-%m-%d_%H:%M:%S`] " : Data refresh job failed" >> ${logfile}
	exit 2
fi