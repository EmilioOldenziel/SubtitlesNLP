import pandas as pd
from ast import literal_eval
from math import log
from nltk.metrics.association import BigramAssocMeasures
from nltk.stem.snowball import *

class Collocations():

    def __init__(self, **kwargs):
        self.bigram_measures = BigramAssocMeasures()
        self.total_bigrams = 554100929 # total number of bigrams before spark export
        self.set_names()

    def set_names(self):
        # list of ~94k US babynames to remove (hello, john), (james, bond)
        names = pd.read_csv("data/names.csv", names=['name'])['name'].tolist()
        self.names = [i for i in names if type(i) == str] #remove non strings

    def read_df(self, language):
        return pd.read_parquet(f"data/sg_{language}.parquet")

    def assert_columns(self, df, column_names):
        df_columns = df.columns
        for column in column_names:
            if column not in df_columns:
                raise ValueError(f"Column {column} is not in DataFrame")

    def get_stemmer(self, language):
        stemmers = {
            'en': EnglishStemmer(),
            'de': GermanStemmer(),
            'nl': DutchStemmer(),
            'fr': FrenchStemmer(),
            'es': SpanishStemmer()
        }
        return stemmers[language]

    def stem_column(self, column, language):
        stemmer = self.get_stemmer(language)
        return column.apply(stemmer.stem)

    def stem(self, df, language):
        df.word1 = self.stem_column(df.word1, language)
        df.word2 = self.stem_column(df.word2, language)
        return df.groupby(['word1', 'word2'], as_index=False)['frequency'].sum()

    def get_language_filter(self, language):
        filters = {
            "de": ['ich', 'du', 'sie', 'mir', 'wir', 'ihr', 'uns', 'euch', 'die', 'das', 'der', 'den', 'er', 'und', 'ein', 'eine', 'einer', 'einem', 'mein', 'dein', 'nicht', 'wie', 'wo', 'wann', 'was'],
            "nl": ['ik', 'jij', 'hij', 'zij', 'wij', 'we', 'jullie', 'hun', 'mij', 'me', 'mijn', 'haar', 'hem', 'een', 'de', 'het', 'wie', 'wat', 'waar', 'hoe', 'niet']
        }
        return filters[language]

    def filter_df(self, df):
        self.assert_columns(df, ["word1", "word2"])
        regex = r"[~!@#$%*()-_+=\[\]{}\\\":;,.<>?/|`]"
        df = df.dropna()
        return \
            df[ \
                ~((df.word1.str.len() == 1)|(df.word2.str.len() == 1)) & \
                ~df.word1.str.contains(regex) & \
                ~df.word2.str.contains(regex) & \
                ~df.word1.str.contains(r"\d") & \
                ~df.word2.str.contains(r"\d") & \
                ~df.word1.str.isnumeric() & \
                ~df.word2.str.isnumeric() & \
                (df.word1 != df.word2)
            ]

    def filter_names(self, df):
        return df[
            (df.word1.isin(self.names)|df.word2.isin(self.names))
        ]

    def append_word_frequency(self, df, language):
        wc = pd.read_csv(f"data/word_counts/wc_{language}.csv", lineterminator='\n', names=['word', 'word_frequency'])
        wc["word_frequency"] = pd.to_numeric(wc["word_frequency"])
        df = df.merge(wc, left_on='word1', right_on='word', how='left') \
            .merge(wc, left_on='word2', right_on='word', how='left') \
            .drop(['word_x', 'word_y'], axis=1) \
            .rename(index=str, columns={"word_frequency_x": "word_1_frequency", "word_frequency_y": "word_2_frequency"})

        return df \
        .query("word_1_frequency >= frequency") \
        .query("word_2_frequency >= frequency")

    def calculate_pmi(self, df):
        self.assert_columns(df, ['frequency', 'word_1_frequency', 'word_2_frequency'])
        log2 = lambda n: log(n,2.0)
        df['pmi'] = (df['frequency']*self.total_bigrams).apply(log2) \
            - (df['word_1_frequency']*df['word_2_frequency']).apply(log2)
        return df
    
    def calculate_chi_sq(self, df):
        self.assert_columns(df, ['frequency', 'word_1_frequency', 'word_2_frequency'])
        df['chi'] = df[['frequency', 'word_1_frequency', 'word_2_frequency']] \
            .apply(lambda row: 
                self.bigram_measures.chi_sq(
                    row['frequency'], \
                    (row['word_1_frequency'], row['word_2_frequency']), \
                    self.total_bigrams)
            , axis=1)
        return df

    def calculate_likelihood(self, df):
        self.assert_columns(df, ['frequency', 'word_1_frequency', 'word_2_frequency'])
        df['ll'] = df[['frequency', 'word_1_frequency', 'word_2_frequency']] \
            .apply(lambda row: 
                self.bigram_measures.likelihood_ratio(
                    row['frequency'], \
                    (row['word_1_frequency'], row['word_2_frequency']), \
                    self.total_bigrams)
            , axis=1)
        return df

    def calculate_fisher(self, df):
        self.assert_columns(df, ['frequency', 'word_1_frequency', 'word_2_frequency'])
        df['fisher'] = df[['frequency', 'word_1_frequency', 'word_2_frequency']] \
            .apply(lambda row: 
                self.bigram_measures.fisher(
                    row['frequency'], \
                    (row['word_1_frequency'], row['word_2_frequency']), \
                    self.total_bigrams)
            , axis=1)
        return df

    def skips_hist(self, df):
        self.assert_columns(df, ['skips'])
        df['number_of_skips'] = df['skips'].apply(len)
        df["skips"] = df["skips"].apply(dict)
        df["skip_average"] = df["skips"].apply(lambda s: sum([k*v for k,v in s.items()])/sum(s.values()))
        df["skip_variance"] = df[["skips", "skip_average"]] \
            .apply(lambda row:
                sum([(k*v)- row['skip_average'] for k,v in row['skips'].items()])/sum(row['skips'].values())
            , axis=1)
        return df
    
    def add_symmetry(self, df):
        self.assert_columns(df, ["word1", "word2"])
        reversed_words = df["word2"]+df["word1"]
        df['symmetric'] = (df["word1"]+df["word2"]).isin(reversed_words)
        return df
