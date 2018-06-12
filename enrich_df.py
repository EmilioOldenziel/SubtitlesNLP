import pandas as pd
from collocations import Collocations

col = Collocations()

for language in ['nl', 'fr', 'es']:

    print(f"starting for language: {language}")

    df = pd.read_parquet(f"data/sg_{language}.parquet")

    df = col.filter_df(df)
    df = col.filter_names(df)
    df = col.skips_hist(df)
    df = col.append_word_frequency(df, language)
    df = col.add_symmetry(df)

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