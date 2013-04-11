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


if [ -z "$1" ];then
	echo "at least 1 argument required (dataset)"
	exit;
fi


dataset=$1
rDir="${HOME}/rProject"

cmd="find $rDir -maxdepth 1 -type d -regex '^$rDir/$dataset.*'"
rewriteDirs=`eval $cmd`
while read -r rewriteMethod; do
        echo "adding analysis $rewriteMethod";
		analysisFiles=`find $rDir/output`
		echo $analysisFiles
done <<< "$rewriteDirs"





