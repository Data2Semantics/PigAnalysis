#!/usr/bin/python
from org.apache.pig.scripting import Pig
import sys
from os.path import dirname, basename, splitext


if len(sys.argv) < 3:
    print "at least 2 args required: dataset, and iteration. Optional 3nd arg: query triple file"
    sys.exit(1)


dataset = sys.argv[1]
iteration = sys.argv[2]

origGraph = "%s/%s.nt" % (dataset, dataset)
queryTripleFile = "%s/evaluation/qTriples" % (dataset)
if len(sys.argv) > 3:
    queryTripleFile = sys.argv[3]


outputFile = "%s/evaluation/qTripleWeights/randomSampleBaseline_%s" % (dataset, iteration)

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
origGraph = LOAD '$origGraph' USING NtLoader() AS (sub:chararray, pred:chararray, obj:chararray);

graphConcat = FOREACH origGraph GENERATE StringConcat(sub, '\\t', pred, '\\t', obj) AS triple;
distinctGraphConcat = DISTINCT graphConcat;
randomSampleTriples = FOREACH distinctGraphConcat GENERATE triple, RANDOM() AS ranking;

joinedTriples = JOIN qTriples BY $0, randomSampleTriples BY $0;

cleanedResult = FOREACH joinedTriples GENERATE $1, $2;

rmf $outputFile
STORE cleanedResult INTO '$outputFile' USING PigStorage();
"""


P = Pig.compile(pigScript)
stats = P.bind().runSingle()
