from pyspark import SparkConf, SparkContext
from itertools import combinations, product
from string import punctuation
import sys
import csv
import re

punct = list(punctuation)

def mapper(line,n):
    results = []
    line = line.strip().lower()
    line = re.sub("\s+"," ",line)
    if(len(line.split("> ")) != 2):
        return results
    loc, words = line.split("> ", 1)
    loc += ">"
    words = "".join([i for i in words.replace("j","i").replace("v","u") if i not in punct])
    words = words.strip().split()
    for i in xrange(len(words)):
        words[i] = [words[i]]
    for x in combinations(range(len(words)),n):
        flag = True
        for i in range(len(x)-1):
            if x[i] >= x[i+1]:
                flag = False
                break
        if flag is True:
            tuples = list(product(*[words[i] for i in x]))
            for tup in tuples:
                word = ", ".join(tup)
                results.append([word,[loc]])
    if len(results) == 0:
        print line
    return results

def lemmatize(k, lemma):
    results = []
    wordpair = [i.strip() for i in k[0].split(", ")]
    current_lemmas = []
    for word in wordpair:
        if word in lemma:
            current_lemmas.append(lemma[word])
        else:
            current_lemmas.append([word])
    tuples = list(product(*current_lemmas))
    for tup in tuples:
        word = "{" + ", ".join(tup) + "}"
        results.append([word,k[1]])
    return results

def reduced(a,b):
    print a,b
    return a+b
    # results = []
    # if a is not None:
    #     results += a
    # if b is not None:
    #     results += b
    # return results

def main(sc, inputDir, outputDir, n, lemma):

    text_file = sc.textFile("hdfs://localhost:9000/user/hadoop/%s"%inputDir)

    mapped = text_file.filter(lambda line : len(line.split(">")) != 1)


    mapped = mapped.flatMap(lambda line : mapper(line,n))
    mapped = mapped.filter(lambda x : x != None and len(x) == 2)

    mapped = mapped.flatMap(lambda tup: lemmatize(tup,lemma))
    mapped = mapped.filter(lambda tup: tup != None and len(tup) == 2)
    # print mapped.count()
    reduced = mapped.reduceByKey(lambda a,b : a + b)
    # print reduced.count()
    # data = sorted(reduced.collect(), key = lambda x: len(x[1]), reverse=True)
    # for i in data[:5]:
    #     print i
    reduced.saveAsTextFile("hdfs://localhost:9000/user/hadoop/%s"%outputDir)


if __name__ == "__main__":

    lemma = {}
    f = open("la.lexicon.csv", "rb")
    reader = csv.reader(f)
    for row in reader:
        row = [i.lower() for i in row if i != ""]
        if row[0] not in lemma:
            lemma[row[0]] = []
        if row[2] not in lemma[row[0]]:
            lemma[row[0]].append(row[2])

    # Configure Spark
    conf = SparkConf().setAppName("CO OCCUR")
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)
    inputDir = sys.argv[1]
    outputDir = sys.argv[2]
    n = int(sys.argv[3])
    # Execute Main functionality
    main(sc, inputDir, outputDir, n, lemma)


