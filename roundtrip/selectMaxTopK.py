#!/usr/bin/python
from org.apache.pig.scripting import Pig
import sys

inputFile = "dbp/roundtrip/directed_pagerank.nt"
outputFile = ""
percentage = "0.5"
exactK = 0
onlyWeights = False
if (len(sys.argv) == 1):
    print "arg1: input file (e.g." + inputFile + "), arg2: top-k percentage (e.g. 0.5, or 100n (for a fixed number), or 0.5w (only retrieve weights, for 50% of graph). arg3(optional): output file"
if len(sys.argv) > 1:
    inputFile = sys.argv[1]
if len(sys.argv) > 2:
    if (sys.argv[2][-1:] == "n"):
        exactK = int(sys.argv[2][:-1])
    elif sys.argv[2][-1:] == "w":
        percentage = sys.argv[2][:-1]
        onlyWeights = True
    else:
        percentage = sys.argv[2]
if len(sys.argv) > 3:
    outputFile = sys.argv[3]
else:
    outputFile = inputFile + "_" + sys.argv[2] + ".nt"
tripleSizeFile = inputFile + "_size"
    
pigScript = """
REGISTER datafu/dist/datafu-0.0.9-SNAPSHOT.jar;
DEFINE UnorderedPairs datafu.pig.bags.UnorderedPairs();
DEFINE FirstTupleFromBag datafu.pig.bags.FirstTupleFromBag();
REGISTER d2s4pig/target/d2s4pig-1.0.jar
DEFINE NtLoader com.data2semantics.pig.loaders.NtLoader();
DEFINE LONGHASH com.data2semantics.pig.udfs.LongHash();
"""

pigScript += """
rankedTriples = LOAD '$inputFile' USING PigStorage() AS (sub:chararray, pred:chararray, obj:chararray, ranking:double);
triplesDistinct = DISTINCT rankedTriples;---to reduce size. there might be some redundant triples
rankedTriplesGrouped = group triplesDistinct all;"""

pigScript += """
orderedTriples = ORDER triplesDistinct BY ranking DESC;"""

if exactK > 0:
    pigScript += """
storeTriples = LIMIT orderedTriples """ + str(exactK) + """;"""
else:
    pigScript += """
tripleCount = foreach rankedTriplesGrouped generate COUNT(triplesDistinct) as count;
rmf $tripleSizeFile
STORE tripleCount INTO '$tripleSizeFile' USING PigStorage();
limitTriples = LIMIT orderedTriples (int)(tripleCount.count * $percentage);


limitTriplesGrouped = group limitTriples all;
minRanking = FOREACH limitTriplesGrouped GENERATE MIN(limitTriples.ranking) AS val;

--ok, so we already have 50%, and we now the minimum ranking value in our set
--remove the tuples containing this minimum ranking value
filteredTriples = FILTER limitTriples BY ranking != (double)minRanking.val;
"""
    if onlyWeights:
        pigScript += """
storeTriples = FOREACH filteredTriples GENERATE $3 ;"""
    else:
        pigScript += """
storeTriples = FOREACH filteredTriples GENERATE $0, $1, $2, '.' ;"""
    

pigScript += """
rmf $outputFile
STORE storeTriples INTO '$outputFile' USING PigStorage();"""

P = Pig.compile(pigScript)
stats = P.bind().runSingle()
