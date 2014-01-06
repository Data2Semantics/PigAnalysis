#!/usr/bin/python
from org.apache.pig.scripting import Pig
import sys
from os.path import dirname, basename, splitext

rankingsFile = "dbp/analysis/dbp_s-o_unweighted_noLit/directed_indegree"
outputFile = "dbp/roundtrip/dbp_s-o_unweighted_noLit/directed_indegree"

if (len(sys.argv) < 2):
	print "takes as argument the analysis file to rewrite. optional arg: output file"
	sys.exit(1)

rankingsFile = sys.argv[1]
#aggregateMethod = sys.argv[2] (disable this arg: always take max
aggregateMethod = "max"
dataset=rankingsFile.split("/")[0]

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

rankedResources = LOAD '$rankingsFile' USING PigStorage() AS (resource:chararray, ranking:double);

subGroup = JOIN distinctTriples by sub LEFT OUTER, rankedResources by resource;
--- generates: subGroup: {group: chararray,triples: {(sub: chararray,pred: chararray,obj: chararray)},rankedResources: {(resource: chararray,ranking: double)}}
----rankedSubTriples = FOREACH subGroup GENERATE FLATTEN(distinctTriples), FLATTEN(rankedResources.ranking) AS subRank;
---generates: rankedSubTriples: {triples::sub: chararray,triples::pred: chararray,triples::obj: chararray,subRank: double}

objGroup = JOIN subGroup by obj LEFT OUTER, rankedResources by resource;
---rankedObjTriples = FOREACH objGroup GENERATE FLATTEN(rankedSubTriples), FLATTEN(rankedResources.ranking) AS objRank;
"""

if aggregateMethod == "avg":
	pigScript += """
rankedTriples = FOREACH objGroup GENERATE 
		$0,$1,$2,
		AVG({($4 is null? 0F: $4),($6 is null? 0F: $6)}) AS ranking;"""
elif aggregateMethod == "max":
	pigScript += """
rankedTriples = FOREACH objGroup GENERATE 
		$0,$1,$2,
		MAX({($4 is null? 0F: $4),($6 is null? 0F: $6)}) AS ranking;"""
elif aggregateMethod == "min":
	pigScript += """
rankedTriples = FOREACH objGroup GENERATE 
		$0,$1,$2,
		MIN({($4 is null? 1F: $4),($6 is null? 1F: $6)}) AS ranking;"""
else: 
	pigScript += """
WRONGGGG. how to aggregate?!"""

pigScript += """

rmf $outputFile
STORE rankedTriples INTO '$outputFile' USING PigStorage();
"""


P = Pig.compile(pigScript)
stats = P.bind().runSingle()
