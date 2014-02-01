#!/usr/bin/python
from org.apache.pig.scripting import Pig
import sys

if len(sys.argv) <= 1:
    print "at least 1 arg required (dataset)"
    sys.exit(1)
dataset = sys.argv[1]
inputQtripleFile = "%s/evaluation/qTriples" % (dataset)
outputFile = "%s/evaluation/randomlyWeightedQtriples"  % (dataset)
    

pigScript = """
graph = LOAD '$inputQtripleFile' USING TextLoader() AS (triple:chararray);


randomlyWeightedTriples = FOREACH graph GENERATE triple, RANDOM();


STORE randomlyWeightedTriples INTO '$outputFile' USING PigStorage();
"""

P = Pig.compile(pigScript)
stats = P.bind().runSingle()
