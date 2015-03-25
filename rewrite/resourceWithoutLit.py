#!/usr/bin/python
from org.apache.pig.scripting import Pig
import sys
from os.path import dirname, basename, splitext

inputFile = "dbp/dbp.nt"

if len(sys.argv) <= 1:
    print "at least 1 arg required (input .nt file)"
    sys.exit(1)

if len(sys.argv) > 1:
    inputFile = sys.argv[1]

outputFile = "%s/rewrite/resourceWithoutLit" % (dirname(inputFile))
if (len(sys.argv) == 3):
    outputFile = sys.argv[2]
pigScript = """
REGISTER datafu/dist/datafu-0.0.9-SNAPSHOT.jar;
DEFINE UnorderedPairs datafu.pig.bags.UnorderedPairs();
REGISTER d2s4pig/target/d2s4pig-1.0.jar
DEFINE NtLoader com.data2semantics.pig.loaders.NtLoader();
DEFINE LONGHASH com.data2semantics.pig.udfs.LongHash();
"""

pigScript += """rdfGraph = LOAD '$inputFile' USING NtLoader() AS (sub:chararray, pred:chararray, obj:chararray);
filteredGraph = FILTER rdfGraph BY (SUBSTRING(obj, 0, 1) != '"');
rdfDistinct = DISTINCT filteredGraph;---to reduce size. there might be some redundant triples
"""
#
#pigScript += """
#filteredGraph1 = filter rdfGraph by sub is not null;
#filteredGraph2 = filter filteredGraph1 by pred is not null;
#filteredGraph = filter filteredGraph2 by obj is not null;
#"""

pigScript += """rewrittenGraph = FOREACH rdfDistinct GENERATE sub, obj, 1;
rmf $outputFile
STORE rewrittenGraph INTO '$outputFile' USING PigStorage();
"""


P = Pig.compile(pigScript)
stats = P.bind().runSingle()
