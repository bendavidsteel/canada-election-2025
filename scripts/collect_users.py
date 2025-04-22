import asyncio
import collections
import datetime
import logging
import os

import polars as pl
from pytok.tiktok import PyTok, NotAvailableException, TimeoutException, NoContentException
from tqdm import tqdm

hashtag_name = 'romania'

def concat(df, dicts):
    try:
        df = pl.concat([df, pl.DataFrame(dicts)], how='diagonal_relaxed')
    except (pl.exceptions.SchemaError, pl.exceptions.PanicException):
        df = pl.DataFrame(df.to_dicts() + dicts, infer_schema_length=len(df) + len(dicts))
    return df


async def main():
    seedlist_df = pl.DataFrame()
    for filename in os.listdir('./data'):
        if filename.endswith('_collection.csv'):
            file_df = pl.read_csv(f'./data/{filename}')
            collection_name = filename.split('_')[0].title()
            file_df = file_df.with_columns(pl.lit(collection_name).alias('Collection'))
            seedlist_df = pl.concat([seedlist_df, file_df], how='diagonal_relaxed')

    seedlist_df = seedlist_df.unique('Tiktok')

    collection_videos = {}
    for collection in seedlist_df['Collection'].unique():
        df_path = f'./data/{collection.lower()}_videos.parquet.zstd'
        if os.path.exists(df_path):
            df = pl.read_parquet(df_path)
        else:
            df = pl.DataFrame()
        collection_videos[collection] = df

    pbar = tqdm(total=len(seedlist_df))
    async with PyTok(manual_captcha_solves=True, logging_level=logging.DEBUG) as api:
        for author in seedlist_df.to_dicts():
            pbar.update(1)
            try:
                handle = author['Tiktok'].replace('@', '')
                user = api.user(username=handle)
                user_info = await user.info()

                # if user_info['followerCount'] < 10000:
                #     continue

                collection_video_df = collection_videos[author['Collection']]
                if 'isPinnedItem' in collection_video_df.columns:
                    author_video_df = collection_video_df.filter((pl.col('author').struct.field('uniqueId') == handle) & (pl.col('isPinnedItem').is_null()))
                else:
                    author_video_df = collection_video_df.filter(pl.col('author').struct.field('uniqueId') == handle)
                if len(author_video_df) > 0:
                    limit_date = author_video_df.with_columns(pl.from_epoch(pl.col('createTime')))['createTime'].max()
                else:
                    limit_date = datetime.datetime(2022, 1, 1)

                videos = []
                async for video in user.videos(count=1000):
                    video_info = await video.info()
                    create_date = datetime.datetime.fromtimestamp(video_info['createTime'])
                    if (not video_info.get('isPinnedItem', False)) and create_date < limit_date:
                        break
                    videos.append(video_info)

                if len(videos) == 0:
                    continue

                collection_videos[author['Collection']] = concat(collection_video_df, videos)

                for collection, df in collection_videos.items():
                    df.write_parquet(os.path.join(f'./data/{collection.lower()}_videos.parquet.zstd'), compression='zstd')
            except (NotAvailableException, TimeoutException, NoContentException) as ex:
                print(f"Exception when fetching user: {author['Tiktok']}, exception: {ex}")

            

if __name__ == "__main__":
    asyncio.run(main())