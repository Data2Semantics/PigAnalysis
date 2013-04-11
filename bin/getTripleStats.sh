#!/bin/bash
if [ -z "$1" ];then
        echo "at least 1 argument required (dataset)"
        exit;
fi

scriptsFile="$HOME/rProject/scripts/getNodeStats.R"
dataset=$1
rDir="${HOME}/rProject"
tripleWeightsDir="$HOME/stats/tripleWeights"
plotsDir="$HOME/stats/plots/tripleWeightDist"
scriptsFile="$HOME/rProject/scripts/getTripleStats.R"

cmd="find $tripleWeightsDir/$dataset* -maxdepth 1 -type f"
tripleWeightFiles=`eval $cmd`
outputRunScript="$HOME/.rRunScript.R"
while read -r tripleWeightFile; do
	basename=`basename $tripleWeightFile`
	targetFile="$plotsDir/$basename.pdf"
    echo "filename <- \"$tripleWeightFile\"" > $outputRunScript;
    echo "outputPdf <- \"$targetFile\"" >> $outputRunScript;
    cat $scriptsFile >> $outputRunScript;
    R -f $outputRunScript;
done <<< "$tripleWeightFiles"
