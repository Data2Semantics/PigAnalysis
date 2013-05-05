#!/usr/bin/python
from org.apache.pig.scripting import Pig
import sys

inputFile = ""
outputFile = ""
percentage = "0.5"
onlyWeights = False
if (len(sys.argv) != 4):
    print "arg1: input file (e.g." + inputFile + "), arg2: outputFile, arg3: top-k percentage (e.g. 0.5)"
if len(sys.argv) > 1:
    inputFile = sys.argv[1]
if len(sys.argv) > 2:
    outputFile = sys.argv[2]
if len(sys.argv) > 3:
    percentage = sys.argv[3]

    
pigScript = """
REGISTER datafu/dist/datafu-0.0.9-SNAPSHOT.jar;
DEFINE UnorderedPairs datafu.pig.bags.UnorderedPairs();
REGISTER d2s4pig/target/d2s4pig-1.0.jar
DEFINE NtLoader com.data2semantics.pig.loaders.NtLoader();
DEFINE LONGHASH com.data2semantics.pig.udfs.LongHash();
"""

pigScript += """
largeGraph = LOAD '$inputFile' USING NtLoader() AS (sub:chararray, pred:chararray, obj:chararray);
rdfDistinct = DISTINCT largeGraph;---to reduce size. there might be some redundant triples

---get counts for a given resource
subGrouped = GROUP rdfDistinct BY sub;
subCounts = FOREACH subGroup GENERATE group AS resource, COUNT(rdfDistinct) AS count;

predGrouped = GROUP rdfDistinct BY sub;
predCounts = FOREACH predGrouped GENERATE group AS resource, COUNT(rdfDistinct) AS count;

objGrouped = GROUP rdfDistinct BY sub;
objCounts = FOREACH objGroup GENERATE group AS resource, COUNT(rdfDistinct) AS count;

countUnion = UNION subCounts, predCounts, objCounts;
unionGrouped = GROUP countUnion BY $0;
resourceCounts = FOREACH unionGrouped GENERATE group AS resource, SUM(countUnion.$1) AS count;

---get to a triple weight
triplesSubGrouped = JOIN rdfDistinct BY sub LEFT OUTER, resourceCounts BY resource; 
triplesPredGrouped = JOIN triplesSubGrouped BY pred LEFT OUTER, resourceCounts BY resource; 
triplesObjGrouped = JOIN triplesPredGrouped BY sub LEFT OUTER, resourceCounts BY resource; 

weightedTriples = FOREACH triplesObjGrouped {
    subCount = $4;
    predCount = $6;
    objCount = $8;
    tripleWeight = $4 + $6 + $8;
    
    GENERATE sub AS sub, pred AS pred, obj AS obj, tripleWeight AS tripleWeight
}

---limit our results
orderedTriples = ORDER weightedTriples BY tripleWeight DESC;
rdfGrouped = group rdfDistinct all;;
tripleCount = foreach rdfGrouped generate COUNT(rdfDistinct) as count;

limitTriples = LIMIT orderedTriples (int)(tripleCount.count * $percentage);

limitTriplesGrouped = group limitTriples all;
minRanking = FOREACH limitTriplesGrouped GENERATE MIN(limitTriples.ranking) AS val;

--ok, so we already have 50%, and we now the minimum ranking value in our set
--remove the tuples containing this minimum ranking value
filteredTriples = FILTER limitTriples BY tripleWeight != minRanking.val;

storeTriples = FOREACH filteredTriples GENERATE $0, $1, $2, '.' ;
distinctTriples = DISTINCT storeTriples;

rmf $outputFile
STORE distinctTriples INTO '$outputFile' USING PigStorage();
"""

P = Pig.compile(pigScript)
stats = P.bind().runSingle()
