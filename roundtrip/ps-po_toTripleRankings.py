#!/usr/bin/python
#pig -x local testPig/dbp.nt testPig/output/directed_pagerank testPig/rewritten_pagerank;
from org.apache.pig.scripting import Pig
import sys

origGraph = "dbp/dbp.nt"
rankingsFile = "dbp/analysis/dbp_ps-po_unweighted/directed_indegree"
outputFile = "dbp/roundtrip/dbp_ps-po_unweighted/directed_indegree"

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

triples = LOAD '$origGraph' USING NtLoader() AS (sub:chararray, pred:chararray, obj:chararray);
triplesDistinct = DISTINCT triples;---to reduce size. there might be some redundant triples
rankedResources = LOAD '$rankingsFile' USING PigStorage() AS (concatenatedResource:chararray, ranking:double);

---rewrite rdf graph again. this way, we can easier joing it with the ranked resources.. (we split the strings again before writing)
rewrittenGraph = FOREACH triplesDistinct {
    GENERATE StringConcat(pred, '@#@#', sub) AS lhs, StringConcat(pred, '@#@#', obj) AS rhs;
} 


lhsGroup = COGROUP rewrittenGraph by lhs, rankedResources by concatenatedResource;
rankedLhs = FOREACH lhsGroup GENERATE FLATTEN(rewrittenGraph), FLATTEN(rankedResources.ranking) AS lhsRank;

rhsGroup = COGROUP rankedLhs by rhs, rankedResources by concatenatedResource;
rankedRhs = FOREACH rhsGroup GENERATE FLATTEN(rankedLhs), FLATTEN(rankedResources.ranking) AS rhsRank;

---rankedObjTriples: {rankedSubTriples::triples::sub: chararray,rankedSubTriples::triples::pred: chararray,rankedSubTriples::triples::obj: chararray,rankedSubTriples::subRank: double,objRank: double}
rankedTriples = FOREACH rankedRhs GENERATE 
		rankedLhs::rewrittenGraph::lhs AS lhs, 
		rankedLhs::rewrittenGraph::rhs AS rhs,
		AVG({(rankedLhs::lhsRank is null? 0F: rankedLhs::lhsRank),(rhsRank is null? 0F: rhsRank)}) AS ranking;


---split ranked triples again
splittedRankedTriples = FOREACH rankedTriples GENERATE FLATTEN(STRSPLIT(lhs,'@#@#',2)), FLATTEN(STRSPLIT(rhs,'@#@#',2)), ranking;
rankedTriplesOutput = FOREACH splittedRankedTriples GENERATE $1 AS sub, $0 AS pred, $3 AS obj, $4 AS ranking;

rmf $outputFile
STORE rankedTriplesOutput INTO '$outputFile' USING PigStorage();
"""


P = Pig.compile(pigScript)
stats = P.bind().runSingle()
