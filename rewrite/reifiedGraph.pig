REGISTER datafu/dist/datafu-0.0.9-SNAPSHOT.jar;
DEFINE UnorderedPairs datafu.pig.bags.UnorderedPairs();
REGISTER d2s4pig/target/d2s4pig-1.0.jar
DEFINE NtLoader com.data2semantics.pig.loaders.NtLoader();
DEFINE LONGHASH com.data2semantics.pig.udfs.LongHash();

origGraph = LOAD 'dbp.nt' USING NtLoader() AS (sub:chararray, pred:chararray, obj:chararray);
rdfGraph = DISTINCT origGraph;---to reduce size. there might be some redundant triples

--largeGraph = SAMPLE largeGraph 0.0000001; --0.0000001: 76 items
--largeGraph = SAMPLE largeGraph 0.001; --0.0001: 75745 items
--dump largeGraph;

/**
 * CREATING A NETWORK OF (SPO) NODES
 */
--what we want: link the tuples in our original bag to each other
--s1, p1, o2 --> s1, p2, o2
subGroup = GROUP rdfGraph BY sub;
/*
creates: 
("sub1",{("sub1","pred1","obj1"),("sub1","pred1","obj2"),("sub1","pred3","obj3")})
("sub2",{("sub2","pred4","obj4"),("sub2","pred5","obj5"),("sub2","pred6","obj5")})

*/
subGroupBags = FOREACH subGroup GENERATE rdfGraph;
/*
creates: 
({("sub1","pred1","obj1"),("sub1","pred1","obj2"),("sub1","pred3","obj3")})
({("sub2","pred4","obj4"),("sub2","pred5","obj5"),("sub2","pred6","obj5")})

*/
spoSubjectPairs = FOREACH subGroupBags GENERATE UnorderedPairs(rdfGraph);
/*
creates: 
({(("sub1","pred1","obj1"),("sub1","pred1","obj2")),(("sub1","pred1","obj1"),("sub1","pred3","obj3")),(("sub1","pred1","obj2"),("sub1","pred3","obj3"))})
({(("sub2","pred4","obj4"),("sub2","pred5","obj5")),(("sub2","pred4","obj4"),("sub2","pred6","obj5")),(("sub2","pred5","obj5"),("sub2","pred6","obj5"))})
*/
spoSubjectGraph = foreach spoSubjectPairs generate FLATTEN($0);
/*
creates:
(("sub1","pred1","obj1"),("sub1","pred1","obj2"))
(("sub1","pred1","obj1"),("sub1","pred3","obj3"))
(("sub1","pred1","obj2"),("sub1","pred3","obj3"))
(("sub2","pred4","obj4"),("sub2","pred5","obj5"))
(("sub2","pred4","obj4"),("sub2","pred6","obj5"))
(("sub2","pred5","obj5"),("sub2","pred6","obj5"))
 */

predGroup = GROUP rdfGraph BY pred;
predGroupBags = FOREACH predGroup GENERATE rdfGraph;
predPredicatePairs = FOREACH predGroupBags GENERATE UnorderedPairs(rdfGraph);
spoPredicateGraph = foreach predPredicatePairs generate FLATTEN($0);

objGroup = GROUP rdfGraph BY obj;
spoObjectGraph = FOREACH objGroup GENERATE FLATTEN(UnorderedPairs(rdfGraph));
--spoObjectPairs = FOREACH objGroupBags GENERATE UnorderedPairs(rdfGraph);
--spoObjectGraph = foreach spoObjectPairs generate FLATTEN($0);


spoGraph = UNION spoSubjectGraph, spoPredicateGraph, spoObjectGraph;
/*
 creates
 (("sub1","pred1","obj1"),("sub1","pred1","obj2"))
 (("sub1","pred1","obj1"),("sub1","pred3","obj3"))
 (("sub1","pred1","obj2"),("sub1","pred1","obj1"))
 */
 
 
 /*
  * Make weighted graph. Append a numerical value to the tuples, representing how many sub/pred/obj both share
  * Requires:
  * 	Detecting (s1p1o1,s1p2o2) === (s1p2o2,s1p1o1)
  * example above should result in:
 (("sub1","pred1","obj1"),("sub1","pred1","obj2"), 2)
 (("sub1","pred1","obj1"),("sub1","pred3","obj3"), 1)
  */
--bla = FOREACH spoGraph {
--	part1 = CONCAT($0.sub, CONCAT($0.pred, $0.obj));
--	part2 = CONCAT($1.sub, CONCAT($1.pred, $1.obj));
--	--check if there exists something in spoGraph with the inverse
--	generate part1, part2;
--}
--weightedSpoGraph = FOREACH spoGraph GENERATE $0, $1, 1; --just use a weight of 1 for each node for now... (do the thing above later)
--
--

--sorted tuples (lowest hash first)
concatSpoGraph = FOREACH spoGraph {
	sub1 = CONCAT((chararray)$0.sub, '@#@#');
	pred1 = CONCAT((chararray)$0.pred, '@#@#');
	part1 = CONCAT(sub1, CONCAT(pred1, $0.obj));
	part1Hash = (long)LONGHASH(part1);
	
	sub2 = CONCAT((chararray)$1.sub, '@#@#');
	pred2 = CONCAT((chararray)$1.pred, '@#@#');
	part2 = CONCAT(sub2, CONCAT(pred2, $1.obj));
	part2Hash = (long)LONGHASH(part2);
	
	generate (part1Hash < part2Hash? part1: part2) as spo1:chararray, (part1Hash < part2Hash? part2: part1) as spo2:chararray ;
}

--sortedSpoGraphGrouped = GROUP sortedSpoGraph BY (spo1, spo2);
--weightedSpoGraph = FOREACH sortedSpoGraphGrouped GENERATE FLATTEN(group), COUNT(sortedSpoGraph);

distinctSpoGraph = DISTINCT concatSpoGraph;

STORE distinctSpoGraph INTO 'spoGraph' USING PigStorage();









