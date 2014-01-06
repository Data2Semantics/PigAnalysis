#!/usr/bin/python
from org.apache.pig.scripting import Pig
import sys
from os.path import dirname, basename, splitext


if (len(sys.argv) < 2):
	print "takes as argument the analysis file to rewrite. optional arg: output file"
	sys.exit(1);

rankingsFile = sys.argv[1]
if rankingsFile[0] == "/":
    #if input is absolute, make it relative (yes, ugly indeed)
    argList = rankingsFile.split("/")
    argList = argList[3:]
    rankingsFile = ("/").join(argList)

dataset = rankingsFile.split("/")[0]

origGraph = "%s/%s.nt" % (dataset,dataset)


outputFile = "%s/roundtrip/%s" % (dataset, basename(rankingsFile))
if len(sys.argv) > 2:
	outputFile = sys.argv[2]
	
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
	
	GENERATE FLATTEN(explodedResources) AS (lhs:chararray, rhs:chararray), ranking AS ranking;
}

joinedTriples = JOIN distinctTriples BY (sub, obj) LEFT OUTER, cleanedResources BY (lhs, rhs);


outputGraph = FOREACH joinedTriples GENERATE $0, $1, $2, ($5 is null? 0F: $5);
distinctGraph = DISTINCT outputGraph;

rmf $outputFile
STORE distinctGraph INTO '$outputFile' USING PigStorage();

"""


P = Pig.compile(pigScript)
stats = P.bind().runSingle()

