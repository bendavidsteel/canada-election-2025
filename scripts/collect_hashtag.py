import asyncio
import datetime
import json
import os

import polars as pl
from pytok.tiktok import PyTok
from TikTokApi import TikTokApi
from tqdm import tqdm

from utils import concat

class ApiWrapper:
    def __init__(self, lib):
        self.lib = lib
        self.api = None

    async def __aenter__(self):
        if self.lib == 'pytok':
            self.api = PyTok(manual_captcha_solves=True)
            await self.api.__aenter__()
        elif self.lib == 'tiktokapi':
            self.api = TikTokApi()
            await self.api.__aenter__()
            await self.api.create_sessions(ms_tokens=[None], num_sessions=1, sleep_after=3, browser=os.getenv("TIKTOK_BROWSER", "chromium"))
        return self
    
    async def get_hashtag_videos(self, hashtag_name):
        hashtag = self.api.hashtag(name=hashtag_name)

        videos = []
        async for video in hashtag.videos(count=1000):
            video_info = video.as_dict
            videos.append(video_info)

        return videos

    async def __aexit__(self, exc_type, exc, tb):
        await self.api.__aexit__(exc_type, exc, tb)

async def main():
    hashtags = [
        'canadaelection', '51ststate', 'canadapoli', 'canadianpolitics', 'cdnpoli', 'markcarney', 'pierrepoilievre', 'poilievre', 'jagmeetsingh', 'carney', 'elbowsup'
    ]
    hashtags.reverse()

    async with ApiWrapper('pytok') as api:
        for hashtag_name in tqdm(hashtags):
            videos = await api.get_hashtag_videos(hashtag_name)

            df = pl.DataFrame(videos)
            df = df.with_columns(pl.lit(datetime.datetime.today()).alias('scrape_date'))
            file_path = f'./data/hashtag_{hashtag_name}.parquet.zstd'
            print(f'Saving {len(df)} new videos to {file_path}')
            if os.path.exists(file_path):
                existing_df = pl.read_parquet(file_path)
                df = concat(existing_df, df)
            df = df.unique('id')
            df.write_parquet(file_path, compression='zstd')

if __name__ == "__main__":
    asyncio.run(main())
