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
	newResource = (resource matches '.*@#@#.*' ? STRSPLIT(resource, '@#@#', 3).$2: resource);
	subResource = (resource matches '.*@#@#.*' ? STRSPLIT(resource, '@#@#', 3).$0: null);
	
	GENERATE subResource AS subResource, newResource AS resource, ranking AS ranking;
}
cleanedResources = DISTINCT explodedResources;

subJoined = JOIN distinctTriples by sub LEFT OUTER, cleanedResources by resource;


---first join by both subject and object, so we get the right literal
objJoined = JOIN subJoined by ($0, $2) LEFT OUTER, cleanedResources by (subResource, resource);

---then join by regular object
explodedResourcesWithoutSub = FILTER explodedResources BY subResource is null;
objJoinedWithRegObjects = JOIN objJoined by $2 LEFT OUTER, explodedResourcesWithoutSub by resource;

---filteredSubJoin = FILTER joinedTriples BY $2 == $4;


cleanedBag = FOREACH objJoinedWithRegObjects {
	subWeight = $5;
	objWeight = ($8 is null? $11: $8);
	GENERATE $0 as sub, $1 as pred, $2 as obj, subWeight AS subWeight, objWeight AS objWeight; 
}
"""

if aggregateMethod == "avg":
	pigScript += """
rankedTriples = FOREACH cleanedBag GENERATE sub, pred, obj, AVG({(subWeight is null? 0F: subWeight),(objWeight is null? 0F: objWeight)}) AS ranking ;"""
elif aggregateMethod == "max":
	pigScript += """
rankedTriples = FOREACH objJoined GENERATE sub, pred, obj, MAX({(subWeight is null? 0F: subWeight),(objWeight is null? 0F: objWeight)}) AS ranking ;"""
elif aggregateMethod == "min":
	pigScript += """
rankedTriples = FOREACH objJoined GENERATE sub, pred, obj, MIN({(subWeight is null? 0F: subWeight),(objWeight is null? 0F: objWeight)}) AS ranking ;"""
else: 
	pigScript += """
WRONGGGG. how to aggregate?!"""

pigScript += """


rmf $outputFile
STORE rankedTriples INTO '$outputFile' USING PigStorage();

"""


P = Pig.compile(pigScript)
stats = P.bind().runSingle()

