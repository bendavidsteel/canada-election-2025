import os
import matplotlib.pyplot as plt
import polars as pl
import numpy as np
from statsmodels.stats.proportion import proportion_confint

def concat(df_a, df_b):
    try:
        return pl.concat([df_a, df_b], how='diagonal_relaxed')
    except:
        return pl.DataFrame(df_a.to_dicts() + df_b.to_dicts(), infer_schema_length=len(df_a) + len(df_b))


def main():
    df = pl.DataFrame()
    for filename in os.listdir('data'):
        if 'election_videos' in filename or 'hashtag_' in filename or '_videos' in filename:
            try:
                file_df = pl.read_parquet(f'./data/{filename}', columns=['createTime', 'id', 'desc', 'author', 'stats'])
                df = concat(df, file_df)
            except Exception as ex:
                print(f"File: {filename}, ex: {ex}")

    df = df.unique('id')

    pass

if __name__ == '__main__':
    main()