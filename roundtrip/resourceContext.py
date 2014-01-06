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

aggregateMethod = "max"

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
	predicate = (resource matches '.*@#@#.*' ? STRSPLIT(resource, '@#@#', 2).$0: (chararray)null);
	
	GENERATE predicate AS predResource, newResource AS resource, ranking AS ranking;
}
cleanedResources = DISTINCT explodedResources;

subJoined = JOIN distinctTriples by sub LEFT OUTER, cleanedResources by resource;


---first join by both predicate and object, so we get the right literal
objJoined = JOIN subJoined by ($1, $2) LEFT OUTER, cleanedResources by (predResource, resource);

---then join by regular object
explodedResourcesWithoutSub = FILTER explodedResources BY predResource is null;
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
rankedTriples = FOREACH cleanedBag GENERATE sub, pred, obj, MAX({(subWeight is null? 0F: subWeight),(objWeight is null? 0F: objWeight)}) AS ranking ;"""
elif aggregateMethod == "min":
	pigScript += """
rankedTriples = FOREACH cleanedBag GENERATE sub, pred, obj, MIN({(subWeight is null? 0F: subWeight),(objWeight is null? 0F: objWeight)}) AS ranking ;"""
else: 
	pigScript += """
WRONGGGG. how to aggregate?!"""

pigScript += """


rmf $outputFile
STORE rankedTriples INTO '$outputFile' USING PigStorage();

"""


P = Pig.compile(pigScript)
stats = P.bind().runSingle()

