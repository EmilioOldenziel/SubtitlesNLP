# Detecting Seperatable verbs

## Introduction

When one is wants to translate a word in a that was written in a foreign language, it can be the case that the word is part of a certain structure. Because of this the meaning of the individual word can be different then when it is combined with the total context of the structure. Examples of these structures are idioms, expressions or separable-verbs. To improve the translation of a word that is part of such a structure it has to be detected and be notified to the user. To accomplish this, a database of structures has to be created. In this report we describe the methods used to create such a dataset  and present the results.

## Collocations
One method to detect separable verbs is to use collocations, collocations are groups of words that occur often together. To find these group of words we first have to extract them from the dataset. This can be performed using various techniques which include N-grams and skip-grams. To extract the collocations we developed a method that, combines n-grams and skip-grams for 22 word collocations, by multiplying each word with the tail of the sentence after that word. For example in "the brown fox jumps"  the word "brown" is multiplied with ["fox", "jumps"] and zipped with the skip distance to create the collocations list [("brown", "fox", 0), ("brown", "jumps", 1)] which extracts all possible collocations from each sentence. The skips from each sentence are collected in a skips list for later analysis.
After multiplying each word in all sentences, the resulting collocations are then gathered and counted. 
## Association metrics

In the set of collocations $C$, each collocation has it's own frequency $f_cw_1w_2$, we join/add/insert the wordcount for each word $w_1x$, $w_2x$. The total number of collocations $N = sum(f_cw_1w_2: \forall w_1w_2 \in C)$.

#### Frequency
The simplest metric for finding usefull collocations is to look at the frequency that a collocation has in the dataset. A collocation with a high frequency has a lot of evidence to be a collocation that is used a lot in spoken language. But, it also introduces a lot of noise by combining 2 words that have an individual high frequency but don't contain useful information together.

#### Pointwise mutual information
PMI is a association metric that indicates what a word tells about the other. It was introduced by (Church & Hanks, 1990) and is implemented as:

$PMI = \textbf{log}_2(f_cw_1w_2 * N) - \textbf{log}_2(w_1*w_2)$

(church1990word)
(Manning and Schütze, 1999)

#### Contingency
To calculate chi-squared and the likelihood-ratio we first have to calculate the contingency matrix $O$, which is defined as:

|               | $w_{1}$                       | $\lnot w_{1}$               |
| --------      |:-----------------------------:| :--------------------------:|
| $w_2$         | $f_cw_1w_2$                   | $w_1 - f_cx = f_c\not{w_1}w_2$ |
| $\lnot w_{2}$ | $w_2 - f_cx = f_cw_1\not{w_2}$   | $N$                         |

and where

$f_c\not{w_1}\not{w_2} = N - f_cw_1w_2 - f_cw_1\not{w_2} - f_c\not{w_1}w_2$
#### Persons chi-squared
Chi-squared is a metric to indicate the dependence when comparing to the expected results $E$ and is defined as 

$\chi^2 = \sum_{i,j}{\frac{(O_{i,j}-E_{i,j})^2}{E_{i,j}}}$
(Manning and Schütze, 1999)

#### Other
  Other metrics that wher applied where likelihood ratio and Fisher's Exact Test (Petersons, 1996)

# Dataset

The dataset the is used is based on the subtitle data from [opensubtitles](http://www.opensubtitles.org/) of which we use the German, Dutch, French and Spanish subtitles.

### Processing
  To handle the 500GB of subtitle data from the opensubtitle data, single-core processing on commodity hardware has become insufficient. Therefore, multi-core processing is needed to bring the processing time down to a workable amount. To achieve this, the processing was performed on a 64 core cluster using Apache Spark to utilize multi-core processing.

  First the raw subtitle data was read to a DataFrame and cleaned. After this the collocations are computed and grouped with frequency count and the skip list is counted to a histograms.

# Experiments
  The proccessed datasets where..

## Bigram-accociation metrics

## Supervised learning



@book{manning1999foundations,
  title={Foundations of statistical natural language processing},
  author={Manning, Christopher D and Manning, Christopher D and Sch{\"u}tze, Hinrich},
  year={1999},
  publisher={MIT press}
}

@article{church1990word,
  title={Word association norms, mutual information, and lexicography},
  author={Church, Kenneth Ward and Hanks, Patrick},
  journal={Computational linguistics},
  volume={16},
  number={1},
  pages={22--29},
  year={1990},
  publisher={MIT Press}
}

@article{pedersen1996fishing,
  title={Fishing for exactness},
  author={Pedersen, Ted},
  journal={arXiv preprint cmp-lg/9608010},
  year={1996}
}
