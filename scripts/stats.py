import polars as pl

from collect_related_election_videos import filter_related

def main():
    df = pl.read_parquet('./data/fetched_election_videos.parquet.zstd')

    keywords = [
        'canada', 'election', '51ststate', 'poilievre', 'carney', 'jagmeet', 'bernier', 'blanchet'
    ]

    df = filter_related(df, keywords)

    author_df = df.select([
        pl.col('author').struct.field('uniqueId'), 
        pl.col('authorStats').struct.field('followerCount'), 
        pl.col('stats').struct.field('commentCount'), 
        pl.col('stats').struct.field('playCount')
    ]).group_by(['uniqueId'])\
        .agg([pl.col('followerCount').len().alias('videoCount'), pl.col('followerCount').max().alias('followerCount'), pl.col('playCount').sum(), pl.col('commentCount').sum()])
    
    cols = ['videoCount', 'followerCount', 'playCount', 'commentCount']

    for col in cols:
        print(col)
        print(author_df.sort(col, descending=True).head(10))

if __name__ == '__main__':
    main()