#!/usr/bin/python
from org.apache.pig.scripting import Pig
import sys

inputFile = "dbp/roundtrip/directed_pagerank.nt"
percentage = "0.5";
if (len(sys.argv) == 1):
    print "arg1: input file (e.g." + inputFile + "), arg2: top-k percentage (e.g. 50)"
if len(sys.argv) > 1:
    inputFile = sys.argv[1]
if len(sys.argv) > 2:
    percentage = str((double(sys.argv[2] + ".0") / 100.0))
outputFile = inputFile.rsplit('.',1)[0] + "_" + percentage + ".nt" 
 
    
pigScript = """
REGISTER datafu/dist/datafu-0.0.9-SNAPSHOT.jar;
DEFINE UnorderedPairs datafu.pig.bags.UnorderedPairs();
REGISTER d2s4pig/target/d2s4pig-1.0.jar
DEFINE NtLoader com.data2semantics.pig.loaders.NtLoader();
DEFINE LONGHASH com.data2semantics.pig.udfs.LongHash();
"""

pigScript += """
rankedTriples = LOAD '$inputFile' USING PigStorage() AS (sub:chararray, pred:chararray, obj:chararray, ranking:double);

rankedTriplesGrouped = group rankedTriples all;
tripleCount = foreach rankedTriplesGrouped generate COUNT(rankedTriples) as count;



orderedTriples = ORDER rankedTriples BY ranking DESC;

limitTriples = LIMIT orderedTriples (int)(tripleCount.count * $percentage); 

storeTriples = FOREACH limitTriples GENERATE $0, $1, $2, '.' ;
rmf $outputFile
STORE storeTriples INTO '$outputFile' USING PigStorage();
"""

P = Pig.compile(pigScript)
stats = P.bind().runSingle()