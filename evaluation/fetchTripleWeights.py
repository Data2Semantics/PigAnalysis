#!/usr/bin/python
from org.apache.pig.scripting import Pig
import sys
from os.path import dirname, basename, splitext


if len(sys.argv) < 2:
    print "at least 1 arg required: the sample to get weights from. Optional 2nd arg: query triple file"
    sys.exit(1)


sampleFile = sys.argv[1]

if sampleFile[0] == "/":
    #if input is absolute, make it relative (yes, ugly indeed)
    argList = sampleFile.split("/")
    argList = argList[3:]
    sampleFile = ("/").join(argList)

dataset = sampleFile.split("/")[0]


queryTripleFile = "%s/evaluation/qTriples" % (dataset)
if len(sys.argv) > 2:
    queryTripleFile = sys.argv[2]


outputFile = "%s/evaluation/qtriples/%s" % (dataset, basename(sampleFile))

pigScript = """
REGISTER datafu/dist/datafu-0.0.9-SNAPSHOT.jar;
DEFINE UnorderedPairs datafu.pig.bags.UnorderedPairs();
DEFINE MD5 datafu.pig.hash.MD5();
REGISTER d2s4pig/target/d2s4pig-1.0.jar;
DEFINE NtLoader com.data2semantics.pig.loaders.NtLoader();
DEFINE LONGHASH com.data2semantics.pig.udfs.LongHash();
"""

pigScript += """
---load qtriples (this load the tuple as 1 pig field. we want to join on the -tuple- anyway)
qTriples = LOAD '$queryTripleFile' USING TextLoader() AS (triple:chararray);

sampleTriples = LOAD '$sampleFile' USING PigStorage() AS (sub:chararray, pred:chararray, obj:chararray, ranking:float);

sampleTriplesConcat = FOREACH sampleTriples GENERATE StringConcat(sub, '\\t', pred, '\\t', obj) AS triple, ranking;

joinedTriples = JOIN qTriples BY $0, sampleTriplesConcat BY $0;

cleanedResult = FOREACH joinedTriples GENERATE $1, $2;

rmf $outputFile
STORE cleanedResult INTO '$outputFile' USING PigStorage();
"""


P = Pig.compile(pigScript)
stats = P.bind().runSingle()
