#!/bin/bash

if [ -z "$1" ];then
	echo "at least 1 argument required (the analysis file)"
	exit;
fi

topKVariants=(0.5 0.2 100n 0.5w)#last one is strange: it's actually: half the graph, and only retrieve weights...
pigRoundtripDir="$HOME/pigAnalysis/roundtrip"
analysisFile=$1
analysisBasename=`basename $analysisFile`;
rewriteDir1=`dirname $analysisFile`;
rewriteDir=`dirname $rewriteDir1`;#quick and ugly
rewriteBasename=`basename $rewriteDir`
IFS=_ read -a delimited <<< "$rewriteBasename"
dataset=${delimited[0]}
hadoopAnalysisDir="$dataset/analysis/"
hadoopRoundtripDir="$dataset/roundtrip/"
rewriteMethod=${delimited[1]}

targetFilename=$rewriteBasename
targetFilename+="_"
targetFilename+=$analysisBasename

localSubgraphDir="$HOME/load/subgraphs/"
statsDir="$HOME/stats/100n/triples/"
tripleWeightsDir="$HOME/stats/tripleWeights"
echo "Storing file in hadoop fs"
hadoopAnalysisFile="$hadoopAnalysisDir/$targetFilename"
checkForDir=`hadoop fs -ls $hadoopAnalysisFile 2>&1 >/dev/null`;
if [ -z "$checkForDir" ]; then
	echo ""
	hadoop fs -rmr $hadoopAnalysisFile;
fi
hadoop fs -put $analysisFile $hadoopAnalysisFile;


echo "Roundtripping file"
pigScriptFile="$pigRoundtripDir/$rewriteMethod.py"
if [ ! -f $pigScriptFile ]; then
	echo "Could not find pig script to do roundtripping with for rewritemethod $rewriteMethod. I tried $pigScriptFile";
	exit;
fi
pig $pigScriptFile $hadoopAnalysisFile;

hadoopRoundtripFile="$hadoopRoundtripDir/$targetFilename"
for topK in "${topKVariants[@]}"; do
	echo "selecting top-k $topK"
	pig $pigRoundtripDir/selectTopK.py $hadoopRoundtripFile $topK;

	echo "catting results locally"
	topKFile="$targetFilename"
	topKFile+="_"
	topKFile+="$topK.nt"
	
	if [[ $topK =~ n$ ]];then
		hadoop fs -cat $hadoopRoundtripDir/$topKFile/part* > $statsDir/$topKFile;
	elif [[ $topK =~ w$ ]];then
		hadoop fs -cat $hadoopRoundtripDir/$topKFile/part* > $tripleWeightsDir/$topKFile;
		getTripleStats.sh $dataset;
	else
		localTargetDir="$localSubgraphDir/$topKFile";
		localTargetFile="$localTargetDir/$topKFile";
		if [ ! -d $localTargetDir ];then
			mkdir $localTargetDir;
		fi

		hadoop fs -cat $hadoopRoundtripDir/$topKFile/part* > $localTargetFile;
		putDirInVirtuoso.sh $localTargetDir;
	fi
done
checkpoint.sh;


