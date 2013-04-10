REGISTER /home/lrd900/code/datafu/dist/datafu-0.0.9-SNAPSHOT.jar;
DEFINE UnorderedPairs datafu.pig.bags.UnorderedPairs();
REGISTER /home/lrd900/code/d2s4pig/target/d2s4pig-1.0.jar
DEFINE NtLoader com.data2semantics.pig.loaders.NtLoader();
DEFINE LONGHASH com.data2semantics.pig.udfs.LongHash();

largeGraph = LOAD 'dbp/dbp.nt' USING NtLoader() AS (sub:chararray, pred:chararray, obj:chararray);
rdfGraph = SAMPLE largeGraph 0.5; 
rmf dbp/dbp_sample_0.5.nt
STORE storeTriples INTO 'dbp/dbp_sample_0.5.nt' USING PigStorage();



