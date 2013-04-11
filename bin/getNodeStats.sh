#!/bin/bash
if [ -z "$1" ];then
        echo "at least 1 argument required (dataset)"
        exit;
fi

scriptsFile="$rDir/scripts/getNodeStats.R"
dataset=$1
rDir="${HOME}/rProject"
top100Dir="$HOME/stats/100n/nodes"
plotsDir="$HOME/plots/nodeWeightDist"
cmd="find $rDir -maxdepth 1 -type d -regex '^$rDir/$dataset.*'"
rewriteDirs=`eval $cmd`
while read -r rewriteDir; do
		rewriteMethod=`basename $rewriteDir`
        echo "getting stats for rewrite method $rewriteDir";
        analysisFiles=`find $rewriteDir/output`
        while read -r analysisFile; do
        		targetFile="$rewriteMethod"
        		targetFile+="_"
        		targetFile+="$analysisFile"
		        echo "inputFilename <- \"$analysisFile\"" > $outputRunScript;
		        echo "outputTop100 <- \"$top100Dir/$targetFile\"" >> $outputRunScript;
		        echo "outputPdf <- \"$plotsDir/$targetFile.pdf\"" >> $outputRunScript;
		        cat $scriptsFile >> $outputRunScript;
		        exit;
		        #R -f $outputRunScript;
        done <<< "$analysisFiles"
done <<< "$rewriteDirs"
