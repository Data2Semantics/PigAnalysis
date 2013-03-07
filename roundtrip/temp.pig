---check why <http://www.scai.fraunhofer.de/rdf/entity/ExactQualifier_NormalizedNamedEntity_19139133_ProMiner_DBA000072> has 2 items per triple instead of 3 after loading it in ntloader

REGISTER datafu/dist/datafu-0.0.9-SNAPSHOT.jar;
DEFINE UnorderedPairs datafu.pig.bags.UnorderedPairs();
REGISTER d2s4pig/target/d2s4pig-1.0.jar
DEFINE NtLoader com.data2semantics.pig.loaders.NtLoader();
DEFINE LONGHASH com.data2semantics.pig.udfs.LongHash();


triples = LOAD 'openphacts.nt' USING NtLoader() AS (sub:chararray, pred:chararray, obj:chararray);
triplesFiltered = FILTER triples BY sub is null or pred is null or obj is null;
STORE triplesFiltered INTO 'triplesFiltered' USING PigStorage();
--so, luckily no null values after loading




--lookup orig data for uri which goes wrong
triples = LOAD 'openphacts.nt' USING NtLoader() AS (sub:chararray, pred:chararray, obj:chararray);
triplesFiltered = FILTER triples BY sub matches '.*ExactQualifier_NormalizedNamedEntity_19139133_ProMiner_DBA000072.*' or obj matches '.*ExactQualifier_NormalizedNamedEntity_19139133_ProMiner_DBA000072.*';
STORE triplesFiltered INTO 'triplesFiltered' USING PigStorage();
--returns:
<http://www.scai.fraunhofer.de/rdf/intern/pubmed_subset/medline11n0955.xml_ProMiner>    <http://purl.org/ao/item>       <http://www.scai.fraunhofer.de/rdf/entity/ExactQualifier_NormalizedNamedEntity_19139133_ProMiner_DBA000072>
<http://www.scai.fraunhofer.de/rdf/entity/ExactQualifier_NormalizedNamedEntity_19139133_ProMiner_DBA000072>     <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>     <http://www.scai.fraunhofer.de/prominer/vocabular/pao#NormalizedNamedEntity>
<http://www.scai.fraunhofer.de/rdf/entity/ExactQualifier_NormalizedNamedEntity_19139133_ProMiner_DBA000072>     <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>     <http://purl.org/ao/types/ExactQualifier>
<http://www.scai.fraunhofer.de/rdf/entity/ExactQualifier_NormalizedNamedEntity_19139133_ProMiner_DBA000072>     <http://purl.org/ao/context>  <http://www.scai.fraunhofer.de/rdf/entity/OffsetStartEndTextSelector_19139133_610_621_0.0>
<http://www.scai.fraunhofer.de/rdf/entity/ExactQualifier_NormalizedNamedEntity_19139133_ProMiner_DBA000072>     <http://purl.org/ao/hasTopic> <http://www.scai.fraunhofer.de/rdf/prominer/DBA000072>
<http://www.scai.fraunhofer.de/rdf/entity/ExactQualifier_NormalizedNamedEntity_19139133_ProMiner_DBA000072>     <http://purl.org/ao/onSourceDocument> <http://www.ncbi.nlm.nih.gov/pubmed/19139133>
--So, all items are still here


--go 

