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

outputFile = "%s/rewrite/%s_s-o-litAsLit_unweighted" % (dirname(inputFile), splitext(basename(inputFile))[0])

pigScript = """
REGISTER datafu/dist/datafu-0.0.9-SNAPSHOT.jar;
DEFINE UnorderedPairs datafu.pig.bags.UnorderedPairs();
DEFINE MD5 datafu.pig.hash.MD5();
REGISTER d2s4pig/target/d2s4pig-1.0.jar;
DEFINE NtLoader com.data2semantics.pig.loaders.NtLoader();
DEFINE LONGHASH com.data2semantics.pig.udfs.LongHash();
"""

pigScript += """rdfGraph = LOAD '$inputFile' USING NtLoader() AS (sub:chararray, pred:chararray, obj:chararray);
rdfDistinct = DISTINCT rdfGraph;---to reduce size. there might be some redundant triples
"""

pigScript += """
rewrittenGraph = FOREACH rdfDistinct {
    newObj = (SUBSTRING(obj, 0, 1) == '"' ? StringConcat(sub, pred, obj): obj);
    GENERATE sub, newObj, 1;
}
rmf $outputFile
STORE rewrittenGraph INTO '$outputFile' USING PigStorage();
"""


P = Pig.compile(pigScript)
stats = P.bind().runSingle()
