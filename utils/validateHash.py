#!/usr/bin/python
from org.apache.pig.scripting import Pig
import sys
from os.path import dirname, basename, splitext

dataset = "dbp"

if len(sys.argv) <= 1:
    print "at least 1 arg required (dataset)"
    sys.exit(1)
    
if len(sys.argv) > 1:
    dataset = sys.argv[1]

origFile = "%s/rewrite/%s_resourceUnique" % (dataset, dataset)
hashedFile = "%s_long" % (origFile)


pigScript = """

hashedGraph = LOAD '$hashedFile' USING PigStorage() AS (lhs:long, rhs:long);
origGraph = LOAD '$origFile' USING PigStorage() AS (lhs:chararray, rhs:chararray);

lhsHashed = FOREACH hashedGraph GENERATE lhs;
rhsHashed = FOREACH hashedGraph GENERATE rhs;
hashedResources = UNION lhsHashed, rhsHashed;
hashedResourcesDistinct = DISTINCT hashedResources;


lhsOrig = FOREACH origGraph GENERATE lhs;
rhsOrig = FOREACH origGraph GENERATE rhs;
origResources = UNION lhsOrig, rhsOrig;
origResourcesDistinct = DISTINCT origResources;

groupedOrigResources = group origResourcesDistinct ALL;
origCount = foreach groupedOrigResources generate COUNT(origResourcesDistinct);

groupedHashedResources = group hashedResourcesDistinct ALL;
hashedCount = foreach groupedHashedResources generate COUNT(hashedResourcesDistinct);

rmf tmp;
mkdir tmp;
STORE hashedCount INTO 'tmp/hashedCount' USING PigStorage();
STORE origCount INTO 'tmp/origCount' USING PigStorage();

"""



P = Pig.compile(pigScript)
stats = P.bind().runSingle()
