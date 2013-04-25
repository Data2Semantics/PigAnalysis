#!/usr/bin/python
from org.apache.pig.scripting import Pig
import sys
from os.path import dirname, basename, splitext

origGraph = "dbp/dbp.nt"
rankingsFile = "dbp/analysis/dbp_s-o_unweighted_noLit/directed_indegree"
outputFile = "dbp/roundtrip/dbp_s-o_unweighted_noLit/directed_indegree"

if (len(sys.argv) < 3):
	print "takes as argument the analysis file to rewrite, and how to aggregate ('min', 'max', or 'avg'). optional arg: output file"


rankingsFile = sys.argv[1]
aggregateMethod = sys.argv[2]
dataset=rankingsFile.split("/")[0]

origGraph = "%s/%s.nt" % (dataset,dataset)
outputFile = "%s/roundtrip/%s_%s" % (dataset, basename(rankingsFile), aggregateMethod)
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
rankedResources = LOAD '$rankingsFile' USING PigStorage() AS (resource:chararray, ranking:double);
explodedResources = FOREACH rankedResources {
	newResource = (resource matches '.*@#@#.*' ? STRSPLIT(resource, '@#@#', 2).$1: resource);
	
	
	GENERATE newResource AS resource, ranking AS ranking;
}
cleanedResources = DISTINCT explodedResources;

subJoined = JOIN distinctTriples by sub, cleanedResources by resource;

objJoined = JOIN subJoined by $2, cleanedResources by resource;

---filteredSubJoin = FILTER joinedTriples BY $2 == $4;

"""

if aggregateMethod == "avg":
	pigScript += """
rankedTriples = FOREACH objJoined GENERATE $0 AS sub, $1 AS pred, $2 AS obj, AVG({($4 is null? 0F: $4),($6 is null? 0F: $6)}) AS ranking ;"""
elif aggregateMethod == "max":
	pigScript += """
rankedTriples = FOREACH objJoined GENERATE $0 AS sub, $1 AS pred, $2 AS obj, MAX({($4 is null? 0F: $4),($6 is null? 0F: $6)}) AS ranking ;"""
elif aggregateMethod == "min":
	pigScript += """
rankedTriples = FOREACH objJoined GENERATE $0 AS sub, $1 AS pred, $2 AS obj, MIN({($4 is null? 0F: $4),($6 is null? 0F: $6)}) AS ranking ;"""
else: 
	pigScript += """
WRONGGGG. how to aggregate?!"""

pigScript += """


rmf $outputFile
STORE distinctRankedTriples INTO '$outputFile' USING PigStorage();

"""


P = Pig.compile(pigScript)
stats = P.bind().runSingle()

