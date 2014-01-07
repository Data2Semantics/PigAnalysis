#!/usr/bin/python
from org.apache.pig.scripting import Pig
import sys
from os.path import dirname, basename, splitext


if len(sys.argv) < 2:
    print "at least 1 arg required: the sample to get weights from. optional arg: output file"
    sys.exit(1)


sampleFile = sys.argv[1]

if sampleFile[0] == "/":
    #if input is absolute, make it relative (yes, ugly indeed)
    argList = sampleFile.split("/")
    argList = argList[3:]
    sampleFile = ("/").join(argList)

dataset = sampleFile.split("/")[0]





outputFile = "%s/evaluation/weightDistribution/%s" % (dataset, basename(sampleFile))
if len(sys.argv) > 2:
    outputFile = sys.argv[2]

pigScript = """
REGISTER datafu/dist/datafu-0.0.9-SNAPSHOT.jar;
DEFINE UnorderedPairs datafu.pig.bags.UnorderedPairs();
DEFINE MD5 datafu.pig.hash.MD5();
REGISTER d2s4pig/target/d2s4pig-1.0.jar;
DEFINE NtLoader com.data2semantics.pig.loaders.NtLoader();
DEFINE LONGHASH com.data2semantics.pig.udfs.LongHash();
"""

pigScript += """

sampleTriples = LOAD '$sampleFile' USING PigStorage() AS (sub:chararray, pred:chararray, obj:chararray, ranking:float);
prunedTriples = FOREACH sampleTriples GENERATE ranking;
groupedTriples = GROUP prunedTriples BY ranking;

rankingCount = FOREACH groupedTriples GENERATE group, COUNT(prunedTriples);

rmf $outputFile
STORE rankingCount INTO '$outputFile' USING PigStorage();
"""


P = Pig.compile(pigScript)
stats = P.bind().runSingle()
