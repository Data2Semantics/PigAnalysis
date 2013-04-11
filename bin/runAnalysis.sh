#!/bin/bash

if [ -z "$1" ];then
	echo "at least 1 argument required (dataset)"
	exit;
fi


dataset=$1
rDir="${HOME}/rProject"
scriptsDir="$rDir/scripts"
outputRunScript="${HOME}/.rRunScript.R"

function getDirsToRun {
	dirs=find . -maxdepth 1 -type d -regex '^\./df.*'
	echo $dirs;
}  




