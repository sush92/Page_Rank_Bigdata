from pyspark.sql import SparkSession
import re
from operator import add
import time



def computeContribs(urls, rank):
    #url contribution to rank of other urls
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


def parseNeighbors(urls):
    #parsing
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]


if __name__ == "__main__":
    
    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()

    
    
    #URL  neighbor URL
    #real text file and map
    readbucket = spark.read.text('gs://bucketname/folder/filename.txt').rdd.map(lambda r: r[0])

    # intialize all urls and neighbor urls
    alllinks = readbucket.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()

    
    #adds all url with other urls link and adds ranks to them one
    rank= alllinks.map(lambda url_neighbors: (url_neighbors[0], 1.0))

    #using pagerank algorithm url ranks are update urls
    for iteration in range(int(4)):
        # Calculates URL contributions to the rank of other URLs.

        contribs = links.join(rank).flatMap(
            lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

       
	#Final calculation on url ranks on neighbor url ranks
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank  * 0.85 + 0.15)
    
    # openfile collect all url and save to google cloud bucket
    
    outputstr="URL  RANK\n"
    for (alllinks , rank) in ranks.collect():
        print("%s has rank: %s." % (alllinks , rank))
        result = " ".join([alllinks , str(rank)])
        outputstr=outputstr+result+'\n'
        
    
    filePath='gs://storedo/output/pagerankOut'
    ranks.coalesce(1).saveAsTextFile(filePath)
    spark.stop()


