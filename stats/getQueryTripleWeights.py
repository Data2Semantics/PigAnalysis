#!/usr/bin/python
from org.apache.pig.scripting import Pig
import sys
from os.path import dirname, basename, splitext

queryNtripleFile = ""

if len(sys.argv) < 3:
    print "takes as argument the roundtripped graph, and the ntriple file containing the query triples. Optional argument is output file"

roundtrippedGraph = sys.argv[1]
queryNtripleFile = sys.argv[2]
ntripleBasename = splitext(basename(queryNtripleFile))[0]
ntripleOutputFilename = ntripleBasename.split("_")[-1]


dataset=roundtrippedGraph.split("/")[0]
if len(sys.argv) > 3:
    outputFile = sys.argv[3];
else:
    outputFile = "%s/queryStats/%s_%s" % (dataset, basename(roundtrippedGraph), ntripleOutputFilename)

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


joinedQueryTriples = JOIN distinctGraph BY (sub, pred, obj), queryTriples BY (sub, pred, obj);

resultsToStore = FOREACH joinedQueryTriples GENERATE $0, $1, $2, $3;---triple, with weight

rmf $outputFile
STORE resultsToStore INTO '$outputFile' USING PigStorage();
"""



P = Pig.compile(pigScript)
stats = P.bind().runSingle()
