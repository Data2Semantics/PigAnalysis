#!/usr/bin/python
from org.apache.pig.scripting import Pig
import sys
from os.path import dirname, basename, splitext


if (len(sys.argv) < 2):
    print "takes as argument the rewritten file to create a dictionary for"
    sys.exit(1)


inputFile = sys.argv[1]
outputDict = "%s_dict" % (inputFile)
longOutputFile = "%s_long" % (inputFile)

    


pigScript = """

REGISTER datafu/dist/datafu-0.0.9-SNAPSHOT.jar;
DEFINE UnorderedPairs datafu.pig.bags.UnorderedPairs();
DEFINE Enumerate com.data2semantics.pig.udfs.EnumerateSafe();
REGISTER d2s4pig/target/d2s4pig-1.0.jar
DEFINE NtLoader com.data2semantics.pig.loaders.NtLoader();
DEFINE LONGHASH com.data2semantics.pig.udfs.LongHash();
/*REGISTER piggybank/contrib/piggybank/java/piggybank.jar*/
/*DEFINE LONGHASH org.apache.pig.piggybank.evaluation.string.HashFNV();*/
graph = LOAD '$inputFile' USING PigStorage() AS (lhs:chararray, rhs:chararray);

---use lots of distincts (reduce our bag size as often and much as possible for performance reasons)
distinctGraph = DISTINCT graph;
lhs = FOREACH distinctGraph GENERATE lhs;
rhs = FOREACH distinctGraph GENERATE rhs;
lhsDistinct = DISTINCT lhs;
rhsDistinct = DISTINCT rhs;
resources = UNION lhsDistinct, rhsDistinct;
resourcesDistinct = DISTINCT resources;

---create dictionary
---resourcesGrouped = GROUP resourcesDistinct ALL;
---dictionary = FOREACH resourcesGrouped GENERATE flatten(LONGHASH(resourcesDistinct));
---STORE dictionary INTO '$outputDict' USING PigStorage();
dictionary = FOREACH resourcesDistinct GENERATE $0, LONGHASH($0);
"""

pigScript += """
---we have a dictionary now. map it back to the original edgelist
distinctGraphLhsJoined = JOIN distinctGraph BY lhs, dictionary BY $0;

distinctGraphJoined = JOIN distinctGraphLhsJoined BY rhs, dictionary BY $0;

newGraph = FOREACH distinctGraphJoined GENERATE $3, $5; 
rmf $outputDict
STORE dictionary INTO '$outputDict' USING PigStorage();
rmf $longOutputFile
STORE newGraph INTO '$longOutputFile' USING PigStorage();
"""



P = Pig.compile(pigScript)
stats = P.bind().runSingle()
