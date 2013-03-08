REGISTER datafu/dist/datafu-0.0.9-SNAPSHOT.jar;
DEFINE UnorderedPairs datafu.pig.bags.UnorderedPairs();
REGISTER d2s4pig/target/d2s4pig-1.0.jar
DEFINE NtLoader com.data2semantics.pig.loaders.NtLoader();
DEFINE LONGHASH com.data2semantics.pig.udfs.LongHash();



triples = LOAD 'openphacts.nt_sample_0.1' USING PigStorage() AS (sub:chararray, pred:chararray, obj:chararray);


rankedResources = LOAD 'pagerank_0.1/pagerank_data_10' USING PigStorage() AS (resource:chararray, pagerank:float);

subGroup = COGROUP triples by sub, rankedResources by resource;
--- generates: subGroup: {group: chararray,triples: {(sub: chararray,pred: chararray,obj: chararray)},rankedResources: {(resource: chararray,pagerank: float)}}
rankedSubTriples = FOREACH subGroup GENERATE FLATTEN(triples), FLATTEN(rankedResources.pagerank) AS subPagerank;
---generates: rankedSubTriples: {triples::sub: chararray,triples::pred: chararray,triples::obj: chararray,subPagerank: float}

objGroup = COGROUP rankedSubTriples by obj, rankedResources by resource;
rankedObjTriples = FOREACH objGroup GENERATE FLATTEN(rankedSubTriples), FLATTEN(rankedResources.pagerank) AS objPagerank;


---rankedObjTriples: {rankedSubTriples::triples::sub: chararray,rankedSubTriples::triples::pred: chararray,rankedSubTriples::triples::obj: chararray,rankedSubTriples::subPagerank: float,objPagerank: float}
rankedTriples = FOREACH rankedObjTriples GENERATE 
		rankedSubTriples::triples::sub, 
		rankedSubTriples::triples::pred,
		rankedSubTriples::triples::obj,
		AVG({(rankedSubTriples::subPagerank is null? 0F: rankedSubTriples::subPagerank),(objPagerank is null? 0F: objPagerank)}) AS pagerank;


rankedTriplesGrouped = group rankedTriples all;
tripleCount = foreach rankedTriplesGrouped generate COUNT(rankedTriples) as count;



orderedTriples = ORDER rankedTriples BY pagerank DESC;


limitTriples = LIMIT orderedTriples (int)(tripleCount.count * 0.1); ---10 percent


rmf pagerank_rankedtriples_0.1
STORE limitTriples INTO 'pagerank_rankedtriples_0.1' USING PigStorage();

