#!/usr/bin/python
from org.apache.pig.scripting import Pig
import sys
from os.path import dirname, basename, splitext

rewrittenGraph = "df/rewrite/df_s-o-litAsLit_unweighted"

if (len(sys.argv) < 2):
    print "takes as argument the rewritten graph to perform the analysis on. Optional argument is custom outputfile"




    
rewrittenGraph = sys.argv[1]
if rewrittenGraph[0] == "/":
    #if input is absolute, make it relative (yes, ugly indeed)
    argList = rewrittenGraph.split("/")
    argList = argList[3:]
    rewrittenGraph = ("/").join(argList)

dataset = rewrittenGraph.split("/")[0]
outputFile = "%s/analysis/%s_indegree" % (dataset, basename(rewrittenGraph))

if (len(sys.argv) == 3):
    outputFile = sys.argv[2];

pigScript = """
REGISTER datafu/dist/datafu-0.0.9-SNAPSHOT.jar;
DEFINE UnorderedPairs datafu.pig.bags.UnorderedPairs();
REGISTER d2s4pig/target/d2s4pig-1.0.jar
DEFINE NtLoader com.data2semantics.pig.loaders.NtLoader();
DEFINE LONGHASH com.data2semantics.pig.udfs.LongHash();
"""

pigScript += """
graph = LOAD '$rewrittenGraph' USING PigStorage() AS (lhs:chararray, rhs:chararray);
distinctGraph = DISTINCT graph;

graphGrouped = GROUP distinctGraph BY rhs;

---calc indegree
weightedResources = FOREACH graphGrouped GENERATE group, COUNT(distinctGraph);

---we still need to add resources with indegree 0. we do this below
---intialize all with indegree zero
lhsGraph = FOREACH graph GENERATE lhs, 0;
---combine with our actual indegree
allWeightedResources = UNION weightedResources, lhsGraph;

---this will contain duplicates, so join again, and calculate the sum of the weights (i.e. zero + 'something')
groupedAllWeightedResources = GROUP allWeightedResources BY $0;
distinctAllWeightedResources = FOREACH groupedAllWeightedResources GENERATE group, SUM(allWeightedResources.$1);

rmf $outputFile
STORE distinctAllWeightedResources INTO '$outputFile' USING PigStorage();
"""



P = Pig.compile(pigScript)
stats = P.bind().runSingle()
