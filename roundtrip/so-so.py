#!/usr/bin/python
from org.apache.pig.scripting import Pig
import sys
from os.path import dirname, basename, splitext


if (len(sys.argv) < 3):
	print "takes as argument the analysis file to rewrite, and how to aggregate ('min', 'max', or 'avg' (unused though, no need to aggregate)). optional arg: output file"

rankingsFile = sys.argv[1]
dataset=rankingsFile.split("/")[0]

origGraph = "%s/%s.nt" % (dataset,dataset)
outputFile = "%s/roundtrip/%s" % (dataset, basename(rankingsFile), aggregateMethod)
if len(sys.argv) > 3:
	outputFile = sys.argv[3]
	
pigScript = """
REGISTER datafu/dist/datafu-0.0.9-SNAPSHOT.jar;
DEFINE UnorderedPairs datafu.pig.bags.UnorderedPairs();
REGISTER d2s4pig/target/d2s4pig-1.0.jar
DEFINE NtLoader com.data2semantics.pig.loaders.NtLoader();
DEFINE LONGHASH com.data2semantics.pig.udfs.LongHash();
"""

pigScript += """
triples = LOAD '$origGraph' USING NtLoader() AS (sub:chararray, pred:chararray, obj:chararray);
distinctTriples = DISTINCT triples;
rankedResources = LOAD '$rankingsFile' USING PigStorage() AS (concatResources:chararray, ranking:double);

cleanedResources = FOREACH rankedResources {
	explodedResources = STRSPLIT(concatResources, '@#@#', 2);
	
	GENERATE FLATTEN(explodedResources), ranking AS ranking;
}

joinedTriples = JOIN distinctTriples BY (sub, obj), cleanedResources BY ($0, $1);


outputGraph = FOREACH filteredTriples GENERATE $0, $1, $2, $5;
distinctGraph = DISTINCT outputGraph;

rmf $outputFile
STORE distinctGraph INTO '$outputFile' USING PigStorage();

"""


P = Pig.compile(pigScript)
stats = P.bind().runSingle()

