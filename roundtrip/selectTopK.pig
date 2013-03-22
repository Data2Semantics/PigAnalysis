REGISTER datafu/dist/datafu-0.0.9-SNAPSHOT.jar;
DEFINE UnorderedPairs datafu.pig.bags.UnorderedPairs();
REGISTER d2s4pig/target/d2s4pig-1.0.jar
DEFINE NtLoader com.data2semantics.pig.loaders.NtLoader();
DEFINE LONGHASH com.data2semantics.pig.udfs.LongHash();


rankedTriples = LOAD 'pagerank_tripleswithranking' USING PigStorage() AS (sub:chararray, pred:chararray, obj:chararray, ranking:double);

rankedTriplesGrouped = group rankedTriples all;
tripleCount = foreach rankedTriplesGrouped generate COUNT(rankedTriples) as count;



orderedTriples = ORDER rankedTriples BY ranking DESC;

limitTriples = LIMIT orderedTriples (int)(tripleCount.count * 0.5); ---50 percent

storeTriples = FOREACH limitTriples GENERATE $0, $1, $2, '.' ;
rmf pagerank_rankedtriples_0.5
STORE storeTriples INTO 'pagerank_rankedtriples_0.5' USING PigStorage();