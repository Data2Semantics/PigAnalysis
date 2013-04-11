#!/usr/bin/python
from org.apache.pig.scripting import Pig
import sys

inputFile = "dbp/roundtrip/directed_pagerank.nt"
outputFile = ""
percentage = "0.5"
exactK = 0
onlyWeights = False
if (len(sys.argv) == 1):
    print "arg1: input file (e.g." + inputFile + "), arg2: top-k percentage (e.g. 0.5, or 100n (for a fixed number), or 0.5w (only retrieve weights, for 50% of graph)"
if len(sys.argv) > 1:
    inputFile = sys.argv[1]
if len(sys.argv) > 2:
    if (sys.argv[2][-1:] == "n"):
        exactK = int(sys.argv[2][:-1])
    elif sys.argv[2][-1:] == "w":
        percentage = int(sys.argv[2][:-1])
        onlyWeights = True
    else:
        percentage = sys.argv[2]

outputFile = inputFile + "_" + sys.argv[2] + ".nt"
    
pigScript = """
REGISTER datafu/dist/datafu-0.0.9-SNAPSHOT.jar;
DEFINE UnorderedPairs datafu.pig.bags.UnorderedPairs();
REGISTER d2s4pig/target/d2s4pig-1.0.jar
DEFINE NtLoader com.data2semantics.pig.loaders.NtLoader();
DEFINE LONGHASH com.data2semantics.pig.udfs.LongHash();
"""

pigScript += """
rankedTriples = LOAD '$inputFile' USING PigStorage() AS (sub:chararray, pred:chararray, obj:chararray, ranking:double);

rankedTriplesGrouped = group rankedTriples all;"""

pigScript += """
orderedTriples = ORDER rankedTriples BY ranking DESC;"""

if exactK > 0:
    pigScript += """
storeTriples = LIMIT orderedTriples """ + str(exactK) + """;"""
else:
    pigScript += """
tripleCount = foreach rankedTriplesGrouped generate COUNT(rankedTriples) as count;
limitTriples = LIMIT orderedTriples (int)(tripleCount.count * $percentage);"""
    if onlyWeights:
        pigScript += """
storeTriples = FOREACH limitTriples GENERATE $3 ;"""
    else:
        pigScript += """
storeTriples = FOREACH limitTriples GENERATE $0, $1, $2, '.' ;"""
    

pigScript += """
rmf $outputFile
STORE storeTriples INTO '$outputFile' USING PigStorage();
"""

P = Pig.compile(pigScript)
stats = P.bind().runSingle()
