#!/usr/bin/python
from org.apache.pig.scripting import Pig
import sys

if len(sys.argv) <= 2:
    print "at least 2 args required (dataset and iteration)"
    sys.exit(1)
dataset = sys.argv[1]
iteration = sys.argv[2]
ntripleFile = "%s/%s.nt"  % (dataset, dataset)


outputFile = "%s/baselines/randomSample_%s"  % (dataset, iteration)
    
pigScript = """
REGISTER datafu/dist/datafu-0.0.9-SNAPSHOT.jar;
DEFINE UnorderedPairs datafu.pig.bags.UnorderedPairs();
REGISTER d2s4pig/target/d2s4pig-1.0.jar
DEFINE NtLoader com.data2semantics.pig.loaders.NtLoader();
DEFINE LONGHASH com.data2semantics.pig.udfs.LongHash();
"""

pigScript += """
graph = LOAD '$ntripleFile' USING NtLoader() AS (sub:chararray, pred:chararray, obj:chararray);


randomlyWeightedGraph = FOREACH graph GENERATE sub, pred, obj, RANDOM();


STORE randomlyWeightedGraph INTO '$outputFile' USING PigStorage();
"""

P = Pig.compile(pigScript)
stats = P.bind().runSingle()
