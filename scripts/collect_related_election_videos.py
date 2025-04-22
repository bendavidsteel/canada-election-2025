import asyncio
import datetime
import logging
import os

import polars as pl
from pytok.tiktok import PyTok
from tqdm import tqdm

from utils import concat

def filter_related(df, keywords):
    return df.filter(
        pl.col('desc').str.to_lowercase().str.contains_any(keywords)
    )

async def main():
    hashtag_df = pl.DataFrame()
    for filename in os.listdir('./data'):
        if filename.endswith('.parquet.zstd') and filename.startswith('hashtag_'):
            hashtag_df = concat(hashtag_df, pl.read_parquet(f'./data/{filename}'))

    hashtag_df = hashtag_df.unique('id')
    hashtag_df = hashtag_df.with_columns(pl.col('author').struct.field('uniqueId').alias('author_id'))

    keywords = [
        'canadapoli', 'cdnpolitics', 'elbowsup', 'canadaelection', '51ststate', 'poilievre', 'carney', 'jagmeet', 'bernier', 'blanchet', 'cdnpoli'
    ]

    video_path = f'./data/fetched_election_videos.parquet.zstd'
    related_path = f'./data/related_election_videos.parquet.zstd'
    backup_video_path = f'./data/backup_fetched_election_videos.parquet.zstd'
    backup_related_path = f'./data/backup_related_election_videos.parquet.zstd'
    if os.path.exists(video_path):
        try:
            video_df = pl.read_parquet(video_path)
        except:
            if os.path.exists(backup_video_path):
                video_df = pl.read_parquet(backup_video_path)
            else:
                video_df = pl.DataFrame()
        try:
            related_df = pl.read_parquet(related_path)
        except:
            related_df = pl.read_parquet(backup_related_path)
        to_fetch_df = concat(hashtag_df, related_df)
        if video_df.shape[0] > 0:
            to_fetch_df = to_fetch_df.filter(~pl.col('id').is_in(video_df.select('id')))
    else:
        video_df = pl.DataFrame()
        related_df = pl.DataFrame()
        to_fetch_df = hashtag_df

    num_since_save = 0
    num_since_backup = 0

    pbar = tqdm()
    
    async with PyTok(manual_captcha_solves=False, headless=True, logging_level=logging.DEBUG) as api:
        while len(to_fetch_df) > 0:
            try:
                author_id, video_id = to_fetch_df.select(['author_id', 'id']).rows()[0]
                video = api.video(username=author_id, id=video_id)
                video_info = await video.info()
                videos = []
                related_videos = []
                video_info['scrape_date'] = datetime.datetime.today()
                videos.append(video_info)

                async for video_info in video.related_videos():
                    video_info['scrape_date'] = datetime.datetime.today()
                    related_videos.append(video_info)

                if len(related_videos) == 0:
                    raise Exception("No related videos found")

                # filter to only videos and related videos that contain keywords in the description
                new_related_df = filter_related(pl.DataFrame(related_videos), keywords)

                video_df = concat(video_df, pl.DataFrame(videos)).unique(subset=['id'])
                related_df = concat(related_df, new_related_df).unique(subset=['id'])
                related_df = related_df.unique(subset=['id'])
                video_df = video_df.unique(subset=['id'])

                video_df = filter_related(video_df, keywords)
                related_df = filter_related(related_df, keywords)
                to_fetch_df = filter_related(to_fetch_df, keywords)

                to_fetch_df = to_fetch_df.tail(len(to_fetch_df) - 1)
                to_fetch_df = concat(to_fetch_df, related_df).unique(subset=['id'])

                related_df = related_df.filter(~pl.col('id').is_in(video_df.select('id')))
                to_fetch_df = to_fetch_df.filter(~pl.col('id').is_in(video_df.select('id')))

                pbar.update(1)

                num_since_save += len(new_related_df)
                num_since_backup += len(new_related_df)
                if num_since_save > 10:
                    video_df.write_parquet(video_path, compression='zstd')
                    related_df.write_parquet(related_path, compression='zstd')
                    num_since_save = 0
                if num_since_backup > 100:
                    video_df.write_parquet(backup_video_path, compression='zstd')
                    related_df.write_parquet(backup_related_path, compression='zstd')
                    num_since_backup = 0

                print(f"Number videos: {len(video_df)}, Number related videos: {len(related_df)}, Number to fetch: {len(to_fetch_df)}")
            except Exception as e:
                print(e)
                to_fetch_df = to_fetch_df.tail(len(to_fetch_df) - 1)

if __name__ == "__main__":
    asyncio.run(main())