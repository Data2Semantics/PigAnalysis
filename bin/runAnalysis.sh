#!/bin/bash

if [ -z "$1" ];then
        echo "at least 1 argument required (dataset)"
        exit;
fi


dataset=$1
rDir="${HOME}/rProject"
scriptsFile="$rDir/scripts/runAllAlgs.R"
outputRunScript="${HOME}/.rRunScript.R"


cmd="find $rDir -maxdepth 1 -type d -regex '^$rDir/df.*'"
dirs=`eval $cmd`
while read -r line; do
        echo "Running analysis for $line";
        echo "setwd(\"$line\")" > $outputRunScript;
        cat $scriptsFile >> $outputRunScript;
        R -f $outputRunScript;
done <<< "$dirs"

