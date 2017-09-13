# Data-Processing-With-Spark

This repository contains data processing using Spark for MapReduce as a part of an academic project for data intensive computing. The project required obtaining word cooccurrence (for n-grams, n = 2, 3) with “line” as the scope or context for co- occurrence.  
We not only pick adjacent words but also all words (forward) co-occurring with the current word but within the line.


### Word Count on Classical Latin Text

This activity involved performing multiple pass on the input to obtain a specialized wordount. 

Pass 1: Lemmetization using the lemmas.csv file

Pass 2: Identify the words in the texts by <word <docid, [chapter#, line#]> for two documents. 

Pass 3: Repeat this for multiple documents.


The rough MR algorithm can be descirbed as 

```
for each word in the text

normalize the word spelling by replacing j with i and v with u throughout check lemmatizer

for the normalized spelling of the word

if the word appears in the lemmatizer
	obtain the list of lemmas for this word
	for each lemma, create a key/value pair from the lemma and the location 
    where the word was found
    
else	
	create a key/value pair from the normalized spelling and the location 
    where the word was found
```

The output looked like this

1. Bigram Output
```
('{pronuntio, sinis}', [u'<sen. eld. fr. 1.4>'])
('{iuro, testis2}', [u'<sen. eld. fr. 1.3>'])
('{ego, non}', [u'<sen. eld. fr. 1.2>'])
(u'{inuolaturum, sui}', [u'<sen. eld. fr. 1.4>'])
('{qui, vinus}', [u'<mac. frag 5>', u'<mac. frag 5>'])
('{quidam, hic}', [u'<sen. eld. fr. 1.2>'])
('{vergilius, voco}', [u'<sen. eld. fr. 1.4>'])
('{dico2, qui}', [u'<mac. frag 7>'])
('{seneco, quis2}', [u'<sen. eld. fr. 1.2>', u'<sen. eld. fr. 1.2>', u'<sen. eld. fr. 1.2>'])
```

2. Trigram output
```
('{in, vir, convenio}', [u'<sen. eld. fr. 1.3>'])
('{audax, ut, sequor}', [u'<sen. eld. fr. 1.2>'])
('{non, quippe, vinum}', [u'<mac. frag 5>'])
('{sino, seneco, quocumque}', [u'<sen. eld. fr. 1.2>'])
('{verum2, quis2, filius}', [u'<sen. eld. fr. 1.2>', u'<sen. eld. fr. 1.2>', u'<sen. eld. fr. 1.2>', u'<sen. eld. fr. 1.2>'])
(u'{declamator, seneco, caligo\u201d}', [u'<sen. eld. fr. 1.2>'])
```

The code and instructions to run the code are present in Code folder


### Comparison of runtime for different n-grams

![alt-text](https://raw.githubusercontent.com/monisjaved/Data-Processing-With-Spark/master/charts/comparison.png "comparison image")

### Bigram Scalability Chart
![alt-text](https://raw.githubusercontent.com/monisjaved/Data-Processing-With-Spark/master/charts/bigram.png "bigram scalability")

