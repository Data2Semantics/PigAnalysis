#!/usr/bin/python
from org.apache.pig.scripting import Pig
import sys

needleTriples = "dbp/roundtrip/directed_pagerank.nt"
haystackTriples = ""
outputFile = ""
percentage = "0.5"
exactK = 0
onlyWeights = False
if (len(sys.argv) < 3):
    print "arg1: triple file containing needles (e.g." + inputFile + "), arg2: roundtripped file containing haystack,  arg3: output file (optional)"
if len(sys.argv) > 1:
    needleTriples = sys.argv[1]
if len(sys.argv) > 2:
    haystackTriples = sys.argv[2]
if len(sys.argv) > 3:
    outputFile = sys.argv[3]
else:
    dataset=needleTriples.split("/")[0]
    outputFile = dataset + "/queryStats/" + needleTriples + ".nt"
    
pigScript = """
REGISTER datafu/dist/datafu-0.0.9-SNAPSHOT.jar;
DEFINE UnorderedPairs datafu.pig.bags.UnorderedPairs();
DEFINE FirstTupleFromBag datafu.pig.bags.FirstTupleFromBag();
REGISTER d2s4pig/target/d2s4pig-1.0.jar
DEFINE NtLoader com.data2semantics.pig.loaders.NtLoader();
DEFINE LONGHASH com.data2semantics.pig.udfs.LongHash();
"""

pigScript += """
needleTriples = LOAD '$needleTriples' USING PigStorage() AS (sub:chararray, pred:chararray, obj:chararray);
haystackTriples = LOAD '$haystackTriples' USING PigStorage() AS (sub:chararray, pred:chararray, obj:chararray, ranking:double);
joinedTriples = JOIN needleTriples by (sub, pred, obj), haystackTriples by (sub, pred, obj); 
"""

pigScript += """
rmf $outputFile
STORE storeTriples INTO '$outputFile' USING PigStorage();"""

P = Pig.compile(pigScript)
stats = P.bind().runSingle()
