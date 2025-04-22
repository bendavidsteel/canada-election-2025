import os

import polars as pl

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
                df = concat(df, pl.read_parquet(f'./data/{filename}'))
            except Exception as ex:
                print(f"File: {filename}, ex: {ex}")

    df = df.unique('id')
    keywords = [
        'canadapoli', 'cdnpoli', 'canadaelection', '51ststate', 'poilievre', 'carney', 'jagmeet', 'bernier', 'blanchet'
    ]
    df = df.filter(pl.col('desc').str.contains_any(keywords))
    author_df = df.group_by(pl.col('author').struct.field('uniqueId'))\
        .agg([
            pl.col('id').len().alias('electionVideoCount'),
            pl.col('author').struct.field('nickname').first().alias('nickname'),
            pl.col('author').struct.field('signature').first().alias('signature'),
            pl.col('authorStats').struct.field('followerCount').max().alias('followerCount'),
            pl.col('authorStats').struct.field('videoCount').max().alias('videoCount'),
        ]).sort('electionVideoCount', descending=True)
    author_df.head(200).write_csv('./data/election_accounts.csv')

if __name__ == '__main__':
    main()