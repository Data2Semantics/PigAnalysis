REGISTER datafu/dist/datafu-0.0.9-SNAPSHOT.jar;
DEFINE UnorderedPairs datafu.pig.bags.UnorderedPairs();
REGISTER d2s4pig/target/d2s4pig-1.0.jar
DEFINE NtLoader com.data2semantics.pig.loaders.NtLoader();
DEFINE LONGHASH com.data2semantics.pig.udfs.LongHash();



triples = LOAD 'dbp.nt' USING NtLoader() AS (sub:chararray, pred:chararray, obj:chararray);

rankedResources = LOAD 'pagerank' USING PigStorage() AS (resource:chararray, ranking:double);

subGroup = COGROUP triples by sub, rankedResources by resource;
--- generates: subGroup: {group: chararray,triples: {(sub: chararray,pred: chararray,obj: chararray)},rankedResources: {(resource: chararray,ranking: double)}}
rankedSubTriples = FOREACH subGroup GENERATE FLATTEN(triples), FLATTEN(rankedResources.ranking) AS subRank;
---generates: rankedSubTriples: {triples::sub: chararray,triples::pred: chararray,triples::obj: chararray,subRank: double}

objGroup = COGROUP rankedSubTriples by obj, rankedResources by resource;
rankedObjTriples = FOREACH objGroup GENERATE FLATTEN(rankedSubTriples), FLATTEN(rankedResources.ranking) AS objRank;


---rankedObjTriples: {rankedSubTriples::triples::sub: chararray,rankedSubTriples::triples::pred: chararray,rankedSubTriples::triples::obj: chararray,rankedSubTriples::subRank: double,objRank: double}
rankedTriples = FOREACH rankedObjTriples GENERATE 
		rankedSubTriples::triples::sub, 
		rankedSubTriples::triples::pred,
		rankedSubTriples::triples::obj,
		AVG({(rankedSubTriples::subRank is null? 0F: rankedSubTriples::subRank),(objRank is null? 0F: objRank)}) AS ranking;


rmf rankedTriples
STORE rankedTriples INTO 'pagerank_tripleswithranking' USING PigStorage();

