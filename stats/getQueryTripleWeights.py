#!/usr/bin/python
from org.apache.pig.scripting import Pig
import sys
from os.path import dirname, basename, splitext

roundtrippedGraph = "df/rewrite/df_s-o-litAsLit_unweighted"
queryNtripleFile = ""

if len(sys.argv) < 3:
    print "takes as argument the roundtripped graph, and the ntriple file containing the query triples. Optional argument is output file"

roundtrippedGraph = sys.argv[1]
queryNtripleFile = sys.argv[2]

dataset=roundtrippedGraph.split("/")[0]
if len(sys.argv) > 3:
    outputFile = sys.argv[3];
else:
    outputFile = "%s/queryStats/%s_" % (dataset, basename(rewrittenGraph), basename(queryNtripleFile))

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
graph = LOAD '$roundtrippedGraph' USING PigStorage() AS (sub:chararray, pred:chararray, obj:chararray, ranking:double);
queryTriples = LOAD '$queryNtripleFile' USING NtLoader() AS (sub:chararray, pred:chararray, obj:chararray);


distinctGraph = DISTINCT graph;
distinctTriples = DISTINCT queryTriples;


joinedQueryTriples = JOIN distinctGraph BY (sub, pred, obj), distinctTriples BY (sub, pred, obj);

resultsToStore = FOREACH joinedQueryTriples GENERATE $0, $1, $2, $3;---triple, with weight

rmf $outputFile
STORE resultsToStore INTO '$outputFile' USING PigStorage();


"""



P = Pig.compile(pigScript)
stats = P.bind().runSingle()
