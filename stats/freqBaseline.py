#!/usr/bin/python
from org.apache.pig.scripting import Pig
import sys


if len(sys.argv) <= 1:
    print "at least 1 arg required (dataset)"
    sys.exit(1)
    
dataset = sys.argv[1]


ntripleFile = "%s/%s.nt"  % (dataset, dataset)
outputFile = "%s/roundtrip/freqBaseline"  % (dataset)
    
pigScript = """
REGISTER datafu/dist/datafu-0.0.9-SNAPSHOT.jar;
DEFINE UnorderedPairs datafu.pig.bags.UnorderedPairs();
REGISTER d2s4pig/target/d2s4pig-1.0.jar
DEFINE NtLoader com.data2semantics.pig.loaders.NtLoader();
DEFINE LONGHASH com.data2semantics.pig.udfs.LongHash();
"""

pigScript += """
graph = LOAD '$ntripleFile' USING NtLoader() AS (sub:chararray, pred:chararray, obj:chararray);

---get counts for a given resource
subGrouped = GROUP graph BY sub;
subCounts = FOREACH subGrouped GENERATE group AS resource, COUNT(graph) AS count;

predGrouped = GROUP graph BY pred;
predCounts = FOREACH predGrouped GENERATE group AS resource, COUNT(graph) AS count;

objGrouped = GROUP graph BY obj;
objCounts = FOREACH objGrouped GENERATE group AS resource, COUNT(graph) AS count;

countUnion = UNION subCounts, predCounts, objCounts;
unionGrouped = GROUP countUnion BY $0;
resourceCounts = FOREACH unionGrouped GENERATE group AS resource, SUM(countUnion.$1) AS count;



---get to a triple weight
triplesSubGrouped = JOIN graph BY sub LEFT OUTER, resourceCounts BY resource; 
triplesPredGrouped = JOIN triplesSubGrouped BY pred LEFT OUTER, resourceCounts BY resource; 
triplesObjGrouped = JOIN triplesPredGrouped BY obj LEFT OUTER, resourceCounts BY resource; 

weightedTriples = FOREACH triplesObjGrouped {
    subCount = $4;
    predCount = $6;
    objCount = $8;
    tripleWeight = ($4 + $6 + $8);
    
    GENERATE sub AS sub, pred AS pred, obj AS obj, tripleWeight AS tripleWeight;
}

rmf $outputFile
STORE weightedTriples INTO '$outputFile' USING PigStorage();
"""

P = Pig.compile(pigScript)
stats = P.bind().runSingle()
