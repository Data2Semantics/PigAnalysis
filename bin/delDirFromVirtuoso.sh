#!/bin/bash
if [ -z "$1" ];then
        echo "at least 1 argument required (directory to remove)"
        exit
fi


source /home/OpenPHACTS-Virtuoso/virtuoso-environment.sh;
isqlFile="${HOME}/.isqlCmdFile.sql"


dir=$(readlink -f $1)

echo "Removing $dir from load list"
delDirFromLdList.sh $dir


basename=`basename $dir`
graphname="http://$basename"

echo "clearing graph";
clearVirtuosoGraph.sh $graphname;