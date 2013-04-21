#!/usr/bin/python
from org.apache.pig.scripting import Pig
import sys
from os.path import dirname, basename, splitext

origGraph = "dbp/dbp.nt"
rankingsFile = "dbp/analysis/dbp_s-o_unweighted_noLit/directed_indegree"
outputFile = "dbp/roundtrip/dbp_s-o_unweighted_noLit/directed_indegree"

if (len(sys.argv) != 2):
	print "takes as only argument the analysis file to rewrite"

rankingsFile = sys.argv[1]

dataset=rankingsFile.split("/")[0]

origGraph = "%s/%s.nt" % (dataset,dataset)
outputFile = "%s/roundtrip/%s" % (dataset, basename(rankingsFile))
    
	
pigScript = """
REGISTER datafu/dist/datafu-0.0.9-SNAPSHOT.jar;
DEFINE UnorderedPairs datafu.pig.bags.UnorderedPairs();
REGISTER d2s4pig/target/d2s4pig-1.0.jar
DEFINE NtLoader com.data2semantics.pig.loaders.NtLoader();
DEFINE LONGHASH com.data2semantics.pig.udfs.LongHash();
"""

pigScript += """
triples = LOAD '$origGraph' USING NtLoader() AS (sub:chararray, pred:chararray, obj:chararray);
distinctTriples = DISTINCT rankedTriples;
rankedResources = LOAD '$rankingsFile' USING PigStorage() AS (resource:chararray, ranking:double);
explodedResources = FOREACH rankedResources {
	newResource = (resource matches '.*@#@#.*' ? STRSPLIT(resource, '@#@#', 2).$1: resource);
	
	
	---newObj = (newObjTuple.$1 is null? newObjTuple.$0: newObjTuple.$1);
	----newObjTuple = STRSPLIT(rankedResources.resource, '@#@#', 2);
	---newObj = (newObjTuple.$1 is null? newObjTuple.$0: newObjTuple.$1);
	GENERATE newResource AS resource, ranking AS ranking;
}
cleanedResources = DISTINCT explodedResources;

subGroup = COGROUP distinctTriples by sub, cleanedResources by resource;
--- generates: subGroup: {group: chararray,distinctTriples: {(sub: chararray,pred: chararray,obj: chararray)},cleanedResources: {(resource: chararray,ranking: double)}}
rankedSubTriples = FOREACH subGroup GENERATE FLATTEN(distinctTriples), FLATTEN(cleanedResources.ranking) AS subRank;
---generates: rankedSubTriples: {distinctTriples::sub: chararray,distinctTriples::pred: chararray,distinctTriples::obj: chararray,subRank: double}

objGroup = COGROUP rankedSubTriples by obj, cleanedResources by resource;
rankedObjTriples = FOREACH objGroup GENERATE FLATTEN(rankedSubTriples), FLATTEN(cleanedResources.ranking) AS objRank;

---rankedObjTriples: {rankedSubTriples::triples::sub: chararray,rankedSubTriples::triples::pred: chararray,rankedSubTriples::triples::obj: chararray,rankedSubTriples::subRank: double,objRank: double}
rankedTriples = FOREACH rankedObjTriples GENERATE 
		rankedSubTriples::distinctTriples::sub, 
		rankedSubTriples::distinctTriples::pred,
		rankedSubTriples::distinctTriples::obj,
		AVG({(rankedSubTriples::subRank is null? 0F: rankedSubTriples::subRank),(objRank is null? 0F: objRank)}) AS ranking;
distinctRankedTriples = DISTINCT rankedTriples;

rmf $outputFile
STORE distinctRankedTriples INTO '$outputFile' USING PigStorage();

"""


P = Pig.compile(pigScript)
stats = P.bind().runSingle()

