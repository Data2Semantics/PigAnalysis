#!/bin/bash
if [ -z "$1" ];then
        echo "at least 1 argument required (directory to remove)"
        exit
fi
source /home/OpenPHACTS-Virtuoso/virtuoso-environment.sh;
isqlFile="${HOME}/.isqlCmdFile.sql"
dir=$1


echo "SELECT * FROM ld_list WHERE ll_file LIKE \'*$dir*\'" > $isqlFile;
echo "EXIT;" >> $isqlFile;
echo "" >> $isqlFile;
cat $isqlFile | isql;
