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


cmd="find $tripleWeightsDir/$dataset* -maxdepth 1 -type f"
tripleWeightFiles=`eval $cmd`
echo $tripleWeightFiles
exit;
outputRunScript="$HOME/.rRunScript.R"
while read -r rewriteDir; do
	rewriteMethod=`basename $rewriteDir`
        echo "getting stats for rewrite method $rewriteDir";
        analysisFiles=`find $rewriteDir/output/*`
        while read -r analysisFile; do
			echo "$rewriteMethod"
        		targetFile="$rewriteMethod"
        		targetFile+="_"
        		targetFile+=`basename $analysisFile`
		        echo "inputFilename <- \"$analysisFile\"" > $outputRunScript;
		        echo "outputTop100 <- \"$top100Dir/$targetFile\"" >> $outputRunScript;
		        echo "outputPdf <- \"$plotsDir/$targetFile.pdf\"" >> $outputRunScript;
		        cat $scriptsFile >> $outputRunScript;
		        R -f $outputRunScript;
			exit;
        done <<< "$analysisFiles"
done <<< "$rewriteDirs"
