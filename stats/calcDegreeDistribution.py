#!/usr/bin/python
from org.apache.pig.scripting import Pig
import sys

if len(sys.argv) <= 1:
    print "at least 1 args required (dataset)"
    sys.exit(1)
dataset = sys.argv[1]

indegreeFile = "%s/analysis/resourceSimple_indegree"  % (dataset)
outdegreeFile = "%s/analysis/resourceSimple_outdegree"  % (dataset)

degreeFile = "%s/evaluation/degree"  % (dataset)
degreeDistFile = "%s/evaluation/degreeDist"  % (dataset)

pigScript = """
outdegree = LOAD '$outdegreeFile' USING PigStorage() AS (resource:chararray, ranking:double);
indegree = LOAD '$indegreeFile' USING PigStorage() AS (resource:chararray, ranking:double);


joinedGraph = JOIN outdegree by resource, indegree by resource;

degree = FOREACH joinedGraph GENERATE $0 as resource, SUM({$1,$3}) AS degreeVal;

STORE degree INTO '$degreeFile' USING PigStorage();

degreeGrouped = GROUP degree BY degreeVal;
degreeDist = FOREACH degreeGrouped GENERATE group, COUNT(degree);

STORE degreeDist INTO '$degreeDistFile' USING PigStorage();
"""

P = Pig.compile(pigScript)
stats = P.bind().runSingle()
