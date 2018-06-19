import pandas as pd
from collocations import Collocations

col = Collocations()

for language in ['de', 'nl', 'fr', 'es']:

    print(f"starting for language: {language}")

    # read files with grams
    df = pd.read_parquet(f"data/sg_{language}.parquet")

    df = col.filter_df(df) # filter grams containing symbols and numbers
    df = col.filter_names(df) # filter out grams contianing a person's name
    df = col.skips_hist(df) # format histograms of skips and calculate mean and variance
    df = col.append_word_frequency(df, language) # append word frequency for word1 and word2 as columns
    df = col.add_symmetry(df) # has gram a symmetric twin? e.g (hello, world) -> (world, hello)

    #calculate metrics
    print("calculating chi")
    df = col.calculate_chi_sq(df)
    print("calculating pmi")
    df = col.calculate_pmi(df)
    print("calculating likelihood")
    df = col.calculate_likelihood(df)
    print("calculating fisher")
    df = col.calculate_fisher(df)

    df.to_csv(f"data/enriched/{language}.csv")

    del(df)