#!/usr/bin/python
from org.apache.pig.scripting import Pig
import sys
from os.path import dirname, basename, splitext
"""
input:
www.A.com    1    { (www.B.com), (www.C.com), (www.D.com), (www.E.com) }
www.B.com    1    { (www.D.com), (www.E.com) }
www.C.com    1    { (www.D.com) }
www.D.com    1    { (www.B.com) }
www.E.com    1    { (www.A.com) }
www.F.com    1    { (www.B.com), (www.C.com) }"""



if (len(sys.argv) < 2):
    print "takes as argument the pagerank output file to postprocess (from tmp dir). Optional argument is custom outputfile"
    sys.exit(1)


pagerank = sys.argv[1]
if pagerank[0] == "/":
    pagerank = pagerank.replace("/user/lrietvld/", "")
    
dataset=pagerank.split("/")[0]

inputBasename = basename(pagerank)
strPos = inputBasename.index("pagerank_data");
outputFile = "%s/analysis/%s" % (dataset, inputBasename[:strPos])

if (len(sys.argv) == 3):
    outputFile = sys.argv[2];
    
    
pigScript = """
pagerankInput = LOAD '$pagerank' AS ( url: chararray, pagerank: float, links:{ link: ( url: chararray ) } );
pagerankGrouped = GROUP pagerankInput ALL;
flattenedPagerank = FOREACH pagerankGrouped generate FLATTEN(TOBAG(MAX(pagerankInput.pagerank), MIN(pagerankInput.pagerank))) as pagerank;
joinedPagerank = JOIN flattenedPagerank BY pagerank, pagerankInput BY pagerank;
outputBag = FOREACH joinedPagerank GENERATE pagerankInput::url, flattenedPagerank::pagerank;

rmf $outputFile
STORE outputBag INTO '$outputFile';
"""

P = Pig.compile(pigScript)
stats = P.bind().runSingle()
