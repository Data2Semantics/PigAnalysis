#!/usr/bin/python
from org.apache.pig.scripting import Pig
import sys


inputFile = "dbp/dbp.nt"
outputFile = "dbp/rewrite/s-o_unweighted_litAsNode"

if len(sys.argv) > 1:
    inputFile = sys.argv[1]
if len(sys.argv) > 2:
    outputFile = sys.argv[2]


useLongHash = False
if useLongHash:
    outputFile += "Hashed"
    
pigScript = """
REGISTER datafu/dist/datafu-0.0.9-SNAPSHOT.jar;
DEFINE UnorderedPairs datafu.pig.bags.UnorderedPairs();
REGISTER d2s4pig/target/d2s4pig-1.0.jar
DEFINE NtLoader com.data2semantics.pig.loaders.NtLoader();
DEFINE LONGHASH com.data2semantics.pig.udfs.LongHash();
"""

pigScript += """rdfGraph = LOAD '$inputFile' USING NtLoader() AS (sub:chararray, pred:chararray, obj:chararray);
rdfDistinct = DISTINCT rdfGraph;---to reduce size. there might be some redundant triples
"""

if useLongHash:
    pigScript += """rewrittenGraph = FOREACH rdfDistinct GENERATE $longHash(sub), $longHash(obj), 1;
"""
else:
    pigScript += """rewrittenGraph = FOREACH rdfDistinct GENERATE sub, obj, 1;
"""


pigScript += """
rmf $outputFile
STORE rewrittenGraph INTO '$outputFile' USING PigStorage();
"""


P = Pig.compile(pigScript)
stats = P.bind().runSingle()
