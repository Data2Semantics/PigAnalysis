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

subGroup = COGROUP distinctTriples by sub LEFT OUTER, rankedResources by resource;
--- generates: subGroup: {group: chararray,triples: {(sub: chararray,pred: chararray,obj: chararray)},rankedResources: {(resource: chararray,ranking: double)}}
rankedSubTriples = FOREACH subGroup GENERATE FLATTEN(distinctTriples), FLATTEN(rankedResources.ranking) AS subRank;
---generates: rankedSubTriples: {triples::sub: chararray,triples::pred: chararray,triples::obj: chararray,subRank: double}

objGroup = COGROUP rankedSubTriples by obj LEFT OUTER, rankedResources by resource;
rankedObjTriples = FOREACH objGroup GENERATE FLATTEN(rankedSubTriples), FLATTEN(rankedResources.ranking) AS objRank;
"""

if aggregateMethod == "avg":
	pigScript += """
rankedTriples = FOREACH rankedObjTriples GENERATE 
		rankedSubTriples::distinctTriples::sub, 
		rankedSubTriples::distinctTriples::pred,
		rankedSubTriples::distinctTriples::obj,
		AVG({(rankedSubTriples::subRank is null? 0F: rankedSubTriples::subRank),(objRank is null? 0F: objRank)}) AS ranking;"""
elif aggregateMethod == "max":
	pigScript += """
rankedTriples = FOREACH rankedObjTriples GENERATE 
		rankedSubTriples::distinctTriples::sub, 
		rankedSubTriples::distinctTriples::pred,
		rankedSubTriples::distinctTriples::obj,
		MAX({(rankedSubTriples::subRank is null? 0F: rankedSubTriples::subRank),(objRank is null? 0F: objRank)}) AS ranking;"""
elif aggregateMethod == "min":
	pigScript += """
rankedTriples = FOREACH rankedObjTriples GENERATE 
		rankedSubTriples::distinctTriples::sub, 
		rankedSubTriples::distinctTriples::pred,
		rankedSubTriples::distinctTriples::obj,
		MIN({(rankedSubTriples::subRank is null? 1F: rankedSubTriples::subRank),(objRank is null? 1F: objRank)}) AS ranking;"""
else: 
	pigScript += """
WRONGGGG. how to aggregate?!"""

pigScript += """

rmf $outputFile
STORE rankedTriples INTO '$outputFile' USING PigStorage();
"""


P = Pig.compile(pigScript)
stats = P.bind().runSingle()
