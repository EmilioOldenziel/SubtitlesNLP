import pandas as pd
import numpy as np
from collocations import Collocations
from ast import literal_eval

for language in ["de", "nl"]:

	col = Collocations()

	# read grams with metrics
	df = pd.read_csv(f"data/enriched/{language}.zip", compression='zip')

	# read list with particle verbs
	verben = pd.read_csv(f"data/seperatable verbs/wordlist/{language}.zip", names=["verben"], compression="zip")

	stemmer = col.get_stemmer(language)

	# stem the verbs
	stemmed_verben = verben['verben'].apply(stemmer.stem)

	# select grams that are a particle verb e.g (steig, ein) -> step in
	verb_df = df[ \
			(df.word2+df.word1).apply(stemmer.stem).isin(stemmed_verben) \
			|(df.word1+df.word2).apply(stemmer.stem).isin(stemmed_verben) \
		]

	# features that are selected for classification
	feature_names = ["frequency", "number_of_skips", "skip_variance", "skip_average", "most_frequent_skip", "word_1_frequency", "word_2_frequency", "symmetric", "chi", "pmi", "fisher", "ll"]

	# get features
	v_df = verb_df[feature_names]

	# get list of to filter words
	language_filter = col.get_language_filter(language)

	# filter grams and select non-particle verbs
	filter_df = df[~(df.word1.isin(language_filter) | df.word2.isin(language_filter)) & \
			~((df.word2+df.word1).apply(stemmer.stem).isin(stemmed_verben) | (df.word1+df.word2).apply(stemmer.stem).isin(stemmed_verben))
		][feature_names]

	# add labels
	filter_df = filter_df.assign(label=np.zeros((filter_df.index.size,1)))
	v_df = v_df.assign(label=np.ones((v_df.index.size,1)))

	# create one balanced dataset
	filter_df = filter_df.sample(v_df.index.size)
	df_XY = v_df.append(filter_df).sample(frac=1)

	df_XY.to_csv(f"data/seperatable verbs/{language}_dataset.csv")