#!/bin/bash
function hadoopLs {
	hadoopLs=()
	echo "hadoop fs -ls $1";
	dirListing=`hadoop fs -ls $1`;
	for word in ${dirListing} ; do
 		if [[ $word =~ ^/ ]];then 
	    	hadoopLs+=(${word})
	    fi
	done
}  



rewriteMethods=(s_o_noLit.py s_o_litAsNode)
if [ -z "$1" ];then
	echo "at least 1 argument required (input). (second arg disables catting locally)"
	exit;
fi
dataset=$1
disableCat=$2

#rewrite stuff
for rewriteMethod in "${rewriteMethods[@]}"; do
  pig pigAnalysis/rewrite/${rewriteMethod} $dataset/$dataset.nt;
done

#get all rewritten stuff locally
if [ -z "$disableCat" ]; then
	hadoopLs "$dataset/rewrite";
	for dir in "${hadoopLs[@]}"; do
		basename=`basename "$dir"`;
		targetFilename="~/rProject/$basename"
		echo "Catting for files $basename";
		hadoop fs -cat $dir/part* > $targetFilename;
	done
fi




