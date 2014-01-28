#!/usr/bin/python

#
# part of these stats are based on the bio2rdf dataset metrics
#
from org.apache.pig.scripting import Pig
import sys


if len(sys.argv) <= 1:
    print "at least 1 arg required (dataset)"
    sys.exit(1)

dataset = sys.argv[1]
ntripleFile = "%s/%s.nt"  % (dataset, dataset)

    
pigScript = """
REGISTER datafu/dist/datafu-0.0.9-SNAPSHOT.jar;
DEFINE UnorderedPairs datafu.pig.bags.UnorderedPairs();
REGISTER d2s4pig/target/d2s4pig-1.0.jar
DEFINE NtLoader com.data2semantics.pig.loaders.NtLoader();
DEFINE LONGHASH com.data2semantics.pig.udfs.LongHash();
"""

pigScript += """
ntriples = LOAD '$ntripleFile' USING NtLoader() AS (sub:chararray, pred:chararray, obj:chararray);

---total number of triples
allGrouped = GROUP ntriples ALL;
tripleCount = FOREACH allGrouped GENERATE COUNT(ntriples);

--- number of unique subjects
subs = FOREACH ntriples GENERATE sub;
distinctSubs = DISTINCT subs;
subGrouped = GROUP distinctSubs ALL;
subCount = FOREACH subGrouped GENERATE COUNT(distinctSubs);

--- number of unique predicate
preds = FOREACH ntriples GENERATE pred;
distinctPreds = DISTINCT preds;
predGrouped = GROUP distinctPreds ALL;
predCount = FOREACH predGrouped GENERATE COUNT(distinctPreds);

--- number of unique object
objs = FOREACH ntriples GENERATE obj;
distinctObjs = DISTINCT objs;
objGrouped = GROUP distinctObjs ALL;
objCount = FOREACH objGrouped GENERATE COUNT(distinctObjs);

--- get number of unique types (classes), and how often they are used
typeTriples = FILTER ntriples BY pred == '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>';
---we want 
groupedTypes = GROUP typeTriples BY obj;
typeCounts = FOREACH groupedTypes GENERATE group, COUNT(typeTriples);
allTypes = FOREACH typeTriples GENERATE obj;
distinctTypes = DISTINCT allTypes;


--- get predicates and the number of unique literals they link to
predLitTriples = FILTER ntriples BY SUBSTRING(obj, 0, 1) == '"';
---remove subject, so we can make our set distinct
predLitCombinations = FOREACH predLitTriples GENERATE pred, obj;
distinctPredLitCombinations = DISTINCT predLitCombinations;
groupedPredLitCombinations = GROUP distinctPredLitCombinations BY pred;
predLitCount = FOREACH groupedPredLitCombinations GENERATE group, COUNT(distinctPredLitCombinations);
allLiterals = FOREACH predLitCombinations GENERATE obj;
distinctLiterals = DISTINCT allLiterals;


--- get predicates and the number of unique URIs they link to
predObjTriples = FILTER ntriples BY SUBSTRING(obj, 0, 1) == '<';
---remove subject, so we can make our set distinct
predObjCombinations = FOREACH predObjTriples GENERATE pred, obj;
distinctPredObjCombinations = DISTINCT predObjCombinations;
groupedPredObjCombinations = GROUP distinctPredObjCombinations BY pred;
predObjCount = FOREACH groupedPredObjCombinations GENERATE group, COUNT(distinctPredObjCombinations);




/*
--- get number of unique subjects and object literals for each predicate
---predLitTriples =
litTriplesGroupedByPred = GROUP predLitTriples BY pred;
uniqueSub_Pred_UniqObjLitCounts = FOREACH litTriplesGroupedByPred {
    subs = DISTINCT predLitTriples.sub;
    lits = DISTINCT predLitTriples.obj;
    GENERATE group, COUNT(subs), COUNT(lits);
}

--- get number of unique subjects and object URIs for each predicate
---predLitTriples =
objTriplesGroupedByPred = GROUP predObjTriples BY pred;
uniqueSub_Pred_UniqObjUriCounts = FOREACH objTriplesGroupedByPred {
    subs = DISTINCT predObjTriples.sub;
    objs = DISTINCT predObjTriples.obj;
    GENERATE group, COUNT(subs), COUNT(objs);
}

--- get the number of distinct subject and object types for each predicate
typeTriplesGroupedByPred = GROUP typeTriples BY pred;
typeRelationCounts = FOREACH typeTriplesGroupedByPred {
    subs = DISTINCT typeTriples.sub;
    objs = DISTINCT typeTriples.obj;
    GENERATE group, COUNT(subs), COUNT(objs);
}
*/


rmf $dataset/stats/tripleCount;
STORE tripleCount INTO '$dataset/stats/tripleCount' USING PigStorage();

rmf $dataset/stats/subCount;
STORE subCount INTO '$dataset/stats/subCount' USING PigStorage();

rmf $dataset/stats/predCount;
STORE predCount INTO '$dataset/stats/predCount' USING PigStorage();

rmf $dataset/stats/objCount;
STORE objCount INTO '$dataset/stats/objCount' USING PigStorage();


rmf $dataset/stats/typeCounts;
STORE typeCounts INTO '$dataset/stats/typeCounts' USING PigStorage();

rmf $dataset/stats/typeCount;
STORE distinctTypes INTO '$dataset/stats/typeCount' USING PigStorage();

rmf $dataset/stats/predLitCounts;
STORE predLitCount INTO '$dataset/stats/predLitCounts' USING PigStorage();

rmf $dataset/stats/distinctLiterals;
STORE distinctLiterals INTO '$dataset/stats/distinctLiterals' USING PigStorage();

rmf $dataset/stats/predObjCounts;
STORE predObjCount INTO '$dataset/stats/predObjCounts' USING PigStorage();

/*
rmf $dataset/stats/uniqSub-Pred-UniqLitCounts;
STORE uniqueSub_Pred_UniqObjLitCounts INTO '$dataset/stats/uniqSub-Pred-UniqLitCounts' USING PigStorage();


rmf $dataset/stats/uniqSub-Pred-UniqUriCounts;
STORE uniqueSub_Pred_UniqObjUriCounts INTO '$dataset/stats/uniqSub-Pred-UniqUriCounts' USING PigStorage();

rmf $dataset/stats/typeRelationCounts;
STORE typeRelationCounts INTO '$dataset/stats/typeRelationCounts' USING PigStorage();
*/
"""

P = Pig.compile(pigScript)
stats = P.bind().runSingle()
