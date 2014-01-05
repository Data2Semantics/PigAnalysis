#!/usr/bin/python
from org.apache.pig.scripting import Pig
import sys
from os.path import dirname, basename, splitext

inputFile = "dbp/dbp.nt"

if len(sys.argv) <= 1:
    print "at least 1 arg required (input .nt file). optional arg: output file"
    sys.exit(1)

if len(sys.argv) > 1:
    inputFile = sys.argv[1]

outputFile = "%s/rewrite/path" % (dirname(inputFile))
if (len(sys.argv) == 3):
    outputFile = sys.argv[2]
    
pigScript = """
REGISTER datafu/dist/datafu-0.0.9-SNAPSHOT.jar;
DEFINE UnorderedPairs datafu.pig.bags.UnorderedPairs();
REGISTER d2s4pig/target/d2s4pig-1.0.jar
DEFINE NtLoader com.data2semantics.pig.loaders.NtLoader();
DEFINE LONGHASH com.data2semantics.pig.udfs.LongHash();
"""

pigScript += """
origGraphLhs = LOAD '$inputFile' USING NtLoader() AS (sub:chararray, pred:chararray, obj:chararray);
rdfGraphLhs = DISTINCT origGraphLhs;---to reduce size. there might be some redundant triples
rdfSoGraphLhs = FOREACH rdfGraphLhs GENERATE sub, obj;

origGraphRhs = LOAD '$inputFile' USING NtLoader() AS (sub:chararray, pred:chararray, obj:chararray);
rdfGraphRhs = DISTINCT origGraphRhs;---to reduce size. there might be some redundant triples
rdfSoGraphRhs = FOREACH rdfGraphRhs GENERATE sub, obj;

joinedGraphs = JOIN rdfSoGraphLhs BY obj, rdfSoGraphRhs BY sub;

outputGraph = FOREACH joinedGraphs GENERATE StringConcat($0, '@#@#', $1), StringConcat($2, '@#@#', $3);
distinctOutputGraph = DISTINCT outputGraph;
rmf $outputFile
STORE distinctOutputGraph INTO '$outputFile' USING PigStorage();
"""
#
#pigScript += """
#filteredGraph1 = filter rdfGraph by sub is not null;
#filteredGraph2 = filter filteredGraph1 by pred is not null;
#filteredGraph = filter filteredGraph2 by obj is not null;
#"""

P = Pig.compile(pigScript)
stats = P.bind().runSingle()
