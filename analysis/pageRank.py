#!/usr/bin/python
from org.apache.pig.scripting import Pig
"""
input:
www.A.com    1    { (www.B.com), (www.C.com), (www.D.com), (www.E.com) }
www.B.com    1    { (www.D.com), (www.E.com) }
www.C.com    1    { (www.D.com) }
www.D.com    1    { (www.B.com) }
www.E.com    1    { (www.A.com) }
www.F.com    1    { (www.B.com), (www.C.com) }"""


rewrittenGraph = "ops/rewrite/df_s-o-litAsLit_unweighted"

if (len(sys.argv) < 2):
    print "takes as argument the rewritten graph to perform the analysis on. Optional argument is custom outputfile"

rewrittenGraph = sys.argv[1]

dataset=rewrittenGraph.split("/")[0]

outputFile = "%s/analysis/%s_directed_pagerank" % (dataset, basename(rewrittenGraph))

if (len(sys.argv) == 3):
    outputFile = sys.argv[2];
    
    
P = Pig.compile("""
previous_pagerank = 
    LOAD '$docs_in'
    AS ( url: $inputType, pagerank: float, links:{ link: ( url: $inputType ) } );
/**
Creates 
 <http://rdf.chemspider.com/3442>, 1.0,  {(<http://www.w3.org/2004/02/skos/core#exactMatch>), (<http://bla>)}
*/

outbound_pagerank = 
    FOREACH previous_pagerank 
    GENERATE 
        pagerank / COUNT ( links ) AS pagerank, 
        FLATTEN ( links ) AS to_url; 
/**
Creates:
1.0, <http://bla>
1.0, <http://www.w3.org/2004/02/skos/core#exactMatch>
*/

cogrpd = cogroup outbound_pagerank by to_url, previous_pagerank by url;
/**
creates:
<http://rdf.chemspider.com/3442>, {}, {(<http://rdf.chemspider.com/3442>, 1.0, {(<http://www.w3.org/2004/02/skos/core#exactMatch>), (<http://bla>)})}
*/                   
                      
new_pagerank = 
    FOREACH 
       cogrpd
    GENERATE 
        group AS url, 
        ( 1 - $d ) + $d * SUM (outbound_pagerank.pagerank) AS pagerank, 
        FLATTEN ( previous_pagerank.links ) AS links,
        FLATTEN ( previous_pagerank.pagerank ) AS previous_pagerank;
STORE new_pagerank 
    INTO '$docs_out';
nonulls = filter new_pagerank by previous_pagerank is not null and pagerank is not null;
pagerank_diff = FOREACH nonulls GENERATE ABS ( previous_pagerank - pagerank );

grpall = group pagerank_diff all;
max_diff = foreach grpall generate MAX (pagerank_diff);

STORE max_diff INTO '$max_diff';

""")

d = 0.5 #damping factor
docs_in= "dbp_large.nt_unweightedLitAsNodeGrouped"
out_dir = "dbp_large.nt_pagerank/"
inputType = "chararray" #use long if we have hashed urls
for i in range(10):
    docs_out = out_dir + "pagerank_data_" + str(i + 1)
    max_diff = out_dir + "max_diff_" + str(i + 1)
    Pig.fs("rmr " + docs_out)
    Pig.fs("rmr " + max_diff)
    stats = P.bind().runSingle()
    if not stats.isSuccessful():
        raise 'failed'
    max_diff_value = float(str(stats.result("max_diff").iterator().next().get(0)))
    print "    max_diff_value = " + str(max_diff_value)
    if max_diff_value < 0.01:
        print "done at iteration " + str(i) + ". Cleaning output"
        break
    #max_diff of previous iterations never used, so clean it up
    Pig.fs("rmr " + max_diff) 
    if i > 1:
        #never for 1st iteration! (otherwise we delete original input...
        Pig.fs("rmr " + docs_in)
        
    docs_in = docs_out

