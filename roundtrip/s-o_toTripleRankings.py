#!/usr/bin/python
from org.apache.pig.scripting import Pig
import sys

origGraph = "dbp/dbp.nt"
rankingsFile = "dbp/analysis/dbp_s-o_unweighted_noLit/directed_indegree"
outputFile = "dbp/roundtrip/dbp_s-o_unweighted_noLit/directed_indegree"

if (len(sys.argv) == 1):
	print "arg1: orig graph (e.g." + origGraph + "), arg2: file with rankings (e.g. "+rankingsFile + "), arg3: outputfile (e.g."+outputFile + ")"
if len(sys.argv) > 1:
    origGraph = sys.argv[1]
if len(sys.argv) > 2:
	rankingsFile = sys.argv[2]
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
rankedResources = LOAD '$rankingsFile' USING PigStorage() AS (resource:chararray, ranking:double);

subGroup = COGROUP triples by sub, rankedResources by resource;
--- generates: subGroup: {group: chararray,triples: {(sub: chararray,pred: chararray,obj: chararray)},rankedResources: {(resource: chararray,ranking: double)}}
rankedSubTriples = FOREACH subGroup GENERATE FLATTEN(triples), FLATTEN(rankedResources.ranking) AS subRank;
---generates: rankedSubTriples: {triples::sub: chararray,triples::pred: chararray,triples::obj: chararray,subRank: double}

objGroup = COGROUP rankedSubTriples by obj, rankedResources by resource;
rankedObjTriples = FOREACH objGroup GENERATE FLATTEN(rankedSubTriples), FLATTEN(rankedResources.ranking) AS objRank;

---rankedObjTriples: {rankedSubTriples::triples::sub: chararray,rankedSubTriples::triples::pred: chararray,rankedSubTriples::triples::obj: chararray,rankedSubTriples::subRank: double,objRank: double}
rankedTriples = FOREACH rankedObjTriples GENERATE 
		rankedSubTriples::triples::sub, 
		rankedSubTriples::triples::pred,
		rankedSubTriples::triples::obj,
		AVG({(rankedSubTriples::subRank is null? 0F: rankedSubTriples::subRank),(objRank is null? 0F: objRank)}) AS ranking;


rmf $outputFile
STORE rankedTriples INTO '$outputFile' USING PigStorage();
"""


P = Pig.compile(pigScript)
stats = P.bind().runSingle()
