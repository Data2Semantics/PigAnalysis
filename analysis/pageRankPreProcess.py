#!/usr/bin/python
from org.apache.pig.scripting import Pig
import sys
from os.path import dirname, basename, splitext

rewrittenGraph = "ops/rewrite/df_s-o-litAsLit_unweighted"

if (len(sys.argv) < 2):
    print "takes as argument the rewritten graph to perform the analysis on. Optional argument is custom outputfile"
    sys.exit(1)

rewrittenGraph = sys.argv[1]
if rewrittenGraph[0] == "/":
	rewrittenGraph = rewrittenGraph.replace("/user/lrietvld/", "")

outputFile = "%s/tmp/%s_directed_pagerank" % (rewrittenGraph.split("/")[0], basename(rewrittenGraph))

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

groupedGraph = GROUP graph BY lhs;

outputGraph = FOREACH groupedGraph GENERATE group, 1, graph.rhs;
rmf $outputFile
STORE outputGraph INTO '$outputFile' USING PigStorage();
"""

P = Pig.compile(pigScript)
stats = P.bind().runSingle()
