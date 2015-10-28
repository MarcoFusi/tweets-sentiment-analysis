from pyspark import SQLContext,Row
import re

# Import Tweets from Hive-Impala (NOT WORKING!!)
# file=sc.textFile("/user/hive/warehouse/tweet_text").map(lambda line: line.split(";"))

# Import Tweets from File
file=sc.textFile("/user/cloudera/data/TweetsTesto.txt")\
        .filter(lambda x: ";" in x)\
    .map(lambda line: line.split(";"))

tweets = file.map(lambda line: Row(statusid=line[0],text=line[2]))
#.sample(False, 0.01, 42)

# Import Dictionary
dictionary = sc.textFile("/user/cloudera/data/AFINN-111.txt").map(lambda line: line.split("\t"))
dictionary = dictionary.map(lambda line: Row(word=line[0], score=line[1]))
 
tweets_kv = tweets.filter(lambda x: x.text <> "").map(lambda x: (x.statusid, x.text.lower().split(" ")))

list_words = tweets_kv.collect()

wordsRDD = sc.parallelize([(".",".")])
wordsRDD = wordsRDD.filter(lambda x: "." not in x)
list_kv = []
i = 0
for pair in list_words:
    words = pair[1]
    for word in words:
        if (not re.match('http', word) and re.search('[a-zA-Z]',word)) :
            list_kv.append((word, pair[0]))
        else: list_kv.append(("",pair[0]))

wordsRDD = sc.parallelize(list_kv)

def to_int(x):
  try: return int(x)
  except: return 0

joinRDD = wordsRDD.leftOuterJoin(dictionary).map(lambda x: (x[0],(x[1][0],to_int(x[1][1]))))
sentiment = joinRDD.map(lambda x: (x[1][0], x[1][1])).reduceByKey(lambda x, y : int(x) + int(y))

print joinRDD.count()
print joinRDD.take(10)
print sentiment.count()
print sentiment.take(10)
