#!/usr/bin/python
from org.apache.pig.scripting import Pig
import sys
"""
input:
www.A.com    1    { (www.B.com), (www.C.com), (www.D.com), (www.E.com) }
www.B.com    1    { (www.D.com), (www.E.com) }
www.C.com    1    { (www.D.com) }
www.D.com    1    { (www.B.com) }
www.E.com    1    { (www.A.com) }
www.F.com    1    { (www.B.com), (www.C.com) }"""


rewrittenGraph = "ops/rewrite/df_s-o-litAsLit_unweighted"

if (len(sys.argv) < 2):
    print "takes as argument the rewritten graph to perform the analysis on. Optional argument is custom outputfile"
    sys.exit(1)

rewrittenGraph = sys.argv[1]

dataset=rewrittenGraph.split("/")[0]

outputFile = "%s/tmp/%s_directed_pagerank" % (dataset, basename(rewrittenGraph))

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

outputGraph = FOREACH groupedGraph GENERATE group, 1, graph;
rmf $outputFile
STORE weightedResources INTO '$outputFile' USING PigStorage();
"""

P = Pig.compile(pigScript)
stats = P.bind().runSingle()




