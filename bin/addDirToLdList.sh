#!/bin/bash
if [ -z "$1" ];then
        echo "at least 1 argument required (directory to add)"
        exit
fi
source /home/OpenPHACTS-Virtuoso/virtuoso-environment.sh;
isqlFile="${HOME}/.isqlCmdFile.sql"
dir=$(readlink -f $1)

basename=`basename $dir`
graphname="http://$basename"


echo "ld_dir('$dir','*.nt','$graphname');" > $isqlFile;
echo "EXIT;" >> $isqlFile;
echo "" >> $isqlFile;
cat $isqlFile | isql;

