#!/bin/bash

#pig pigAnalysis/roundtrip/toTripleRankings.py dbp/dbp.nt dbp/analysis/dbp_s-o_unweighted_litAsNode/directed_indegree dbp/roundtrip/dbp_s-o_unweighted_litAsNode/directed_indegree;
pig pigAnalysis/roundtrip/toTripleRankings.py dbp/dbp.nt dbp/analysis/dbp_s-o_unweighted_litAsNode/directed_outdegree dbp/roundtrip/dbp_s-o_unweighted_litAsNode/directed_outdegree;
pig pigAnalysis/roundtrip/toTripleRankings.py dbp/dbp.nt dbp/analysis/dbp_s-o_unweighted_litAsNode/directed_pagerank dbp/roundtrip/dbp_s-o_unweighted_litAsNode/directed_pagerank;
pig pigAnalysis/roundtrip/toTripleRankings.py dbp/dbp.nt dbp/analysis/dbp_s-o_unweighted_litAsNode/undirected_degree dbp/roundtrip/dbp_s-o_unweighted_litAsNode/undirected_degree;
pig pigAnalysis/roundtrip/toTripleRankings.py dbp/dbp.nt dbp/analysis/dbp_s-o_unweighted_litAsNode/undirected_pagerank dbp/roundtrip/dbp_s-o_unweighted_litAsNode/undirected_pagerank;



pig pigAnalysis/roundtrip/toTripleRankings.py dbp/dbp.nt dbp/analysis/dbp_s-o_unweighted_noLit/directed_indegree dbp/roundtrip/dbp_s-o_unweighted_noLit/directed_indegree;
pig pigAnalysis/roundtrip/toTripleRankings.py dbp/dbp.nt dbp/analysis/dbp_s-o_unweighted_noLit/directed_outdegree dbp/roundtrip/dbp_s-o_unweighted_noLit/directed_outdegree;
pig pigAnalysis/roundtrip/toTripleRankings.py dbp/dbp.nt dbp/analysis/dbp_s-o_unweighted_noLit/directed_pagerank dbp/roundtrip/dbp_s-o_unweighted_noLit/directed_pagerank;
pig pigAnalysis/roundtrip/toTripleRankings.py dbp/dbp.nt dbp/analysis/dbp_s-o_unweighted_noLit/undirected_degree dbp/roundtrip/dbp_s-o_unweighted_noLit/undirected_degree;
pig pigAnalysis/roundtrip/toTripleRankings.py dbp/dbp.nt dbp/analysis/dbp_s-o_unweighted_noLit/undirected_pagerank dbp/roundtrip/dbp_s-o_unweighted_noLit/undirected_pagerank;

#pig pigAnalysis/roundtrip/selectTopK.py dbp/roundtrip/dbp_s-o_unweighted_litAsNode/directed_indegree 50;
pig pigAnalysis/roundtrip/selectTopK.py dbp/roundtrip/dbp_s-o_unweighted_litAsNode/directed_outdegree 50;
pig pigAnalysis/roundtrip/selectTopK.py dbp/roundtrip/dbp_s-o_unweighted_litAsNode/directed_pagerank 50;
pig pigAnalysis/roundtrip/selectTopK.py dbp/roundtrip/dbp_s-o_unweighted_litAsNode/undirected_degree 50;
pig pigAnalysis/roundtrip/selectTopK.py dbp/roundtrip/dbp_s-o_unweighted_litAsNode/undirected_pagerank 50;

pig pigAnalysis/roundtrip/selectTopK.py dbp/roundtrip/dbp_s-o_unweighted_noLit/directed_indegree 50;
pig pigAnalysis/roundtrip/selectTopK.py dbp/roundtrip/dbp_s-o_unweighted_noLit/directed_outdegree 50;
pig pigAnalysis/roundtrip/selectTopK.py dbp/roundtrip/dbp_s-o_unweighted_noLit/directed_pagerank 50;
pig pigAnalysis/roundtrip/selectTopK.py dbp/roundtrip/dbp_s-o_unweighted_noLit/undirected_degree 50;
pig pigAnalysis/roundtrip/selectTopK.py dbp/roundtrip/dbp_s-o_unweighted_noLit/undirected_pagerank 50;

pig pigAnalysis/roundtrip/selectTopK.py dbp/roundtrip/dbp_s-o_unweighted_litAsNode/directed_indegree 20;
pig pigAnalysis/roundtrip/selectTopK.py dbp/roundtrip/dbp_s-o_unweighted_litAsNode/directed_outdegree 20;
pig pigAnalysis/roundtrip/selectTopK.py dbp/roundtrip/dbp_s-o_unweighted_litAsNode/directed_pagerank 20;
pig pigAnalysis/roundtrip/selectTopK.py dbp/roundtrip/dbp_s-o_unweighted_litAsNode/undirected_degree 20;
pig pigAnalysis/roundtrip/selectTopK.py dbp/roundtrip/dbp_s-o_unweighted_litAsNode/undirected_pagerank 20;

pig pigAnalysis/roundtrip/selectTopK.py dbp/roundtrip/dbp_s-o_unweighted_noLit/directed_indegree 20;
pig pigAnalysis/roundtrip/selectTopK.py dbp/roundtrip/dbp_s-o_unweighted_noLit/directed_outdegree 20;
pig pigAnalysis/roundtrip/selectTopK.py dbp/roundtrip/dbp_s-o_unweighted_noLit/directed_pagerank 20;
pig pigAnalysis/roundtrip/selectTopK.py dbp/roundtrip/dbp_s-o_unweighted_noLit/undirected_degree 20;
pig pigAnalysis/roundtrip/selectTopK.py dbp/roundtrip/dbp_s-o_unweighted_noLit/undirected_pagerank 20;

pig pigAnalysis/roundtrip/selectTopK.py dbp/roundtrip/dbp_s-o_unweighted_litAsNode/directed_indegree 100n;
pig pigAnalysis/roundtrip/selectTopK.py dbp/roundtrip/dbp_s-o_unweighted_litAsNode/directed_outdegree 100n;
pig pigAnalysis/roundtrip/selectTopK.py dbp/roundtrip/dbp_s-o_unweighted_litAsNode/directed_pagerank 100n;
pig pigAnalysis/roundtrip/selectTopK.py dbp/roundtrip/dbp_s-o_unweighted_litAsNode/undirected_degree 100n;
pig pigAnalysis/roundtrip/selectTopK.py dbp/roundtrip/dbp_s-o_unweighted_litAsNode/undirected_pagerank 100n;

pig pigAnalysis/roundtrip/selectTopK.py dbp/roundtrip/dbp_s-o_unweighted_noLit/directed_indegree 100n;
pig pigAnalysis/roundtrip/selectTopK.py dbp/roundtrip/dbp_s-o_unweighted_noLit/directed_outdegree 100n;
pig pigAnalysis/roundtrip/selectTopK.py dbp/roundtrip/dbp_s-o_unweighted_noLit/directed_pagerank 100n;
pig pigAnalysis/roundtrip/selectTopK.py dbp/roundtrip/dbp_s-o_unweighted_noLit/undirected_degree 100n;
pig pigAnalysis/roundtrip/selectTopK.py dbp/roundtrip/dbp_s-o_unweighted_noLit/undirected_pagerank 100n;


