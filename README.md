Sampling RDF graphs
===========

Collection of PIG scripts for analysis and rewriting of graphs.

###Dependencies

* PIG[3]
* Hadoop[4]
* Python[5]
* Datafu[1]: run `mvn clean package` to compile, and add it to the PIG classpath
* D2S4PIG[2]: run `mvn clean package` to compile, and add it to the PIG classpath

###Howto
Our sampling method consists of several steps:

1. Rewrite the RDF graph to an unlabelled graph (folder `rewrite`). We provide 4 different rewrite methods
2. Analyze the unlabelled graph using standard network analysis algorithms (folder `analysis`)
3. Rewrite these results back to an RDF graph, where the resource weights are aggregated to triple level (folder `roundtrip`)

The pig scripts in each of these folders are wrapped as python scripts. Running a script without arguments will print the documentation for that specific script, and the input/output it provides



####links
1. [https://github.com/linkedin/datafu](https://github.com/linkedin/datafu)
2. [https://github.com/Data2Semantics/D2S4Pig](https://github.com/Data2Semantics/D2S4Pig)
3. [http://pig.apache.org/](http://pig.apache.org/)
4. [http://hadoop.apache.org/](http://hadoop.apache.org/)
5. [http://www.python.org/](http://www.python.org/)
