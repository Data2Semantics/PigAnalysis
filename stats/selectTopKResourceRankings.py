#!/usr/bin/python
from org.apache.pig.scripting import Pig
import sys

inputFile = "dbp/roundtrip/dbp_s-o_unweighted_noLit/directed_indegree/"#ranked triples
outputFile = ""
percentage = "0.5";
exactK = 0
if (len(sys.argv) <= 3):
    print "arg1: input file (e.g." + inputFile + "), arg2: outputfile, arg3: top-k percentage (e.g. 50)"
if len(sys.argv) > 1:
    inputFile = sys.argv[1]
if len(sys.argv) > 2:
    inputFile = sys.argv[1]
if len(sys.argv) > 3:
    if (sys.argv[3][-1:] == "n"):
        exactK = int(sys.argv[3][:-1])
    else:
        percentage = str((float(sys.argv[3] + ".0") / 100.0))
        
if exactK > 0:
    outputFile = inputFile + "_" + str(exactK) + "n"
else:
    outputFile = inputFile + "_" + percentage + ""  
    
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
limitTriples = LIMIT orderedTriples (int)(tripleCount.count * $percentage);
storeTriples = FOREACH limitTriples GENERATE $0, $1, $2, '.' ;"""
    

pigScript += """
rmf $outputFile
STORE storeTriples INTO '$outputFile' USING PigStorage();
"""

P = Pig.compile(pigScript)
stats = P.bind().runSingle()
