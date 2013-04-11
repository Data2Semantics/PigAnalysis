#!/bin/bash
if [ -z "$1" ];then
        echo "at least 1 argument required (directory to remove)"
        exit
fi
source /home/OpenPHACTS-Virtuoso/virtuoso-environment.sh;
isqlFile="${HOME}/.isqlCmdFile.sql"
dir=$(readlink -f $1)

echo "Removing $dir from load list"
echo "DELETE FROM load_list WHERE ll_file LIKE '$dir*';" > $isqlFile;
echo "EXIT;" >> $isqlFile;
echo "" >> $isqlFile;
cat $isqlFile | isql;

