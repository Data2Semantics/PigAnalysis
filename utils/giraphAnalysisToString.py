#!/usr/bin/python
from org.apache.pig.scripting import Pig
import sys
from os.path import dirname, basename, splitext

inputFile = "dbp/dbp.nt"

if (len(sys.argv) < 2):
    print "takes as argument:the weighted list of longs"
    sys.exit(1)
    
    
rankedLongs = sys.argv[1]
if rankedLongs[0] == "/":
    #if input is absolute, make it relative (yes, ugly indeed)
    argList = rankedLongs.split("/")
    argList = argList[3:]
    rankedLongs = ("/").join(argList)

dataset = rankedLongs.split("/")[0]
rewriteMethod = basename(rankedLongs).split("_")[0]
rewritePath = "%s/rewrite/%s" % (dataset, rewriteMethod)
dictPath = "%s_dict" % (rewritePath)

output = rankedLongs[:-5]; #remove the '_long' annotation
pigScript = """

longRankings = LOAD '$rankedLongs' USING PigStorage() AS (resourceId:long, ranking:float);
dict = LOAD '$dictPath' USING PigStorage() AS (dictText:chararray, dictLong:long);

joinedRankings = join longRankings by resourceId, dict by dictLong;

cleanedRankings = foreach joinedRankings GENERATE $2, $1;


rmf $output
STORE cleanedRankings INTO '$output' USING PigStorage();
"""



P = Pig.compile(pigScript)
stats = P.bind().runSingle()
