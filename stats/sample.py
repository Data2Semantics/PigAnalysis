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
rdfDistinct = DISTINCT filteredGraph;---to reduce size. there might be some redundant triples
graphWithRandom = FOREACH rdfDistinct GENERATE sub, pred, obj, RANDOM() AS rand;
graphSortedRandom = ORDER graphWithRandom BY rand;

graphGrouped = group rdfDistinct all;
tripleCount = foreach graphGrouped generate COUNT(rdfDistinct) as count;
limitTriples = LIMIT graphSortedRandom BY tripleCount;

 
ntriples = FOREACH limitTriples GENERATE sub, pred, obj, '.';
rmf $outputFile
STORE ntriples INTO '$outputFile' USING PigStorage();
"""

P = Pig.compile(pigScript)
stats = P.bind().runSingle()
