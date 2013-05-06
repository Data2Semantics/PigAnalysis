#!/usr/bin/python
from org.apache.pig.scripting import Pig
import sys
from os.path import dirname, basename, splitext

rewrittenGraph = "df/rewrite/df_s-o-litAsLit_unweighted"

if (len(sys.argv) < 2):
    print "takes as argument the rewritten graph to perform the analysis on. Optional argument is custom outputfile"

rewrittenGraph = sys.argv[1]
if rewrittenGraph[0] == "/":
        rewrittenGraph = rewrittenGraph.replace("/user/lrietvld/", "")
dataset = rewrittenGraph.split("/")[0]

outputFile = "%s/analysis/%s_directed_indegree" % (dataset, basename(rewrittenGraph))

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

weightedResources = FOREACH graphGrouped GENERATE group, COUNT(distinctGraph);

STORE weightedResources INTO '$outputFile' USING PigStorage();
"""



P = Pig.compile(pigScript)
stats = P.bind().runSingle()
