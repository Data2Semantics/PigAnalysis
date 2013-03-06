REGISTER lib/datafu.jar;
DEFINE UnorderedPairs datafu.pig.bags.UnorderedPairs();
REGISTER lib/d2s4pig-1.0.jar;
DEFINE NtLoader com.data2semantics.pig.loaders.NtLoader();
DEFINE LONGHASH com.data2semantics.pig.udfs.LongHash();



triples = LOAD 'openphacts.nt_sample_0.1' USING NtLoader() AS (sub:chararray, pred:chararray, obj:chararray);



filteredTriples = FILTER triples BY sub is null or pred is null or obj is null;
STORE filteredTriples INTO 'filteredTriples' USING PigStorage();

rankedResources = LOAD 'pagerank_0.1/pagerank_data_10' AS (resource:chararray, pagerank:float);

subGroup = COGROUP triples by sub, rankedResources by resource;
rankedSubTriples = FOREACH subGroup GENERATE FLATTEN(triples), FLATTEN(rankedResources.pagerank) AS subPagerank;

objGroup = COGROUP rankedSubTriples by obj, rankedResources by resource;
rankedObjTriples = FOREACH objGroup GENERATE FLATTEN(rankedSubTriples), FLATTEN(rankedResources.pagerank) AS objPagerank;

rankedTriples = FOREACH rankedObjTriples GENERATE rankedSubTriples.sub, rankedSubTriples.pred, rankedSubTriples.obj, AVG({(subPagerank is null? 0F: subPagerank), (objPagerank is null? 0F: objPagerank)});
STORE rankedTriples INTO 'pagerank_rankedtriples_0.1' USING PigStorage();

