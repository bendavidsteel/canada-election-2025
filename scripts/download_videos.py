import asyncio
import datetime
import json
import logging
import os
import re
import traceback

import boto3
import dotenv
import hydra
import requests
import tqdm
from pytok.tiktok import PyTok

import polars as pl

logger = logging.getLogger(__name__)

def get_video_data(token, endpoint, start_date_str, end_date_str):
    
    if token is None:
        logger.info("Failed to retrieve token.")
        return None

    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "platform": 'tiktok',
        "query": '*',
        "from_date": datetime.datetime.strptime(start_date_str, "%Y-%m-%d").strftime("%d-%m-%Y"),
        "to_date": datetime.datetime.strptime(end_date_str, "%Y-%m-%d").strftime("%d-%m-%Y")
    }

    try:
        with requests.session() as session:
            response = session.post(endpoint, json=params, headers=headers)
            response.raise_for_status()
            return response.json()
    except requests.RequestException as e:
        logger.info(f"Request failed for tiktok from {start_date_str} to {end_date_str}: {e}")
        return None
    
def fetch_platform_data_daily(config, token):
    # Go back the number of days needed to make the hour look-back period
    current_datetime = datetime.date(2022, 1, 1)
    final_datetime = datetime.date.today()
    endpoint = config["meo-api"]["base-url"] + "/dashboard"
    while current_datetime < final_datetime:
        
        day_str = current_datetime.strftime("%Y-%m-%d")
        day_str_plus1 = (current_datetime + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        #logger.info(f"Processing data for {day_str}")
        data = get_video_data(token, endpoint, day_str, day_str_plus1)
        for video_data in data:
            yield video_data
        current_datetime += datetime.timedelta(days=1)  # Move to the next day

def get_headers():
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Accept-Language': 'en-CA,en-US;q=0.7,en;q=0.3',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        # 'Cookie': r'ttwid=1%7CVjAEjqSGTWfEtOgZaKZDivCLcdIwWVF35RBR81OPqCo%7C1717430787%7C474b8becd1744ec061d4184b946a52f4132401f599824b054c9a1c39bce9fafd; tt_chain_token=4bxAAJDwW0q8F/pklMLgYw==; tta_attr_id_mirror=0.1718720483.7381848264502411296; odin_tt=2bb2d49be3766ef28a7b665b3756ec345f21d0157728b51043d84528ade78e88ed35a8366d9e525bf12d36dd963b019503cda1176f9cd205b53a20d710fddeacbe089452af07d4d3d48719f400aeb6dc; msToken=G2uzyCcpanL1WcSQ5MpaI-O8ZizSlWHhyr1Aeraf_UotNH70HYI2BmJdFWBrXQ90-1WNClM7Ss6dgjFbaOfJgx__Nd9IyPTVBEvPMm3mU…5gSES59QgAlOhrGQcpqTNtN7c5HpH4-dcSHYKo47jjLdUwPGnEXbszZEO8DMiQBlkDzB2gYhdM365gKGH8pWX5amAZE6UAU_ryBhsLCZYVDhyzHnUjuy; delay_guest_mode_vid=8; tt_csrf_token=EpvGphBx-s0E5WuL9tNTcJoiRmIWxgDR7tiM; passport_fe_beating_status=false; perf_feed_cache={%22expireTimestamp%22:1745089200000%2C%22itemIds%22:[%227465741377683410192%22%2C%227476086973095070983%22]}; msToken=G2uzyCcpanL1WcSQ5MpaI-O8ZizSlWHhyr1Aeraf_UotNH70HYI2BmJdFWBrXQ90-1WNClM7Ss6dgjFbaOfJgx__Nd9IyPTVBEvPMm3mUpqsyTdNVEfvU8fKSU96nCzYCtGvoWU4eJZI2VO-4ydT',
        'DNT': '1',
        'Host': 'www.tiktok.com',
        'Pragma': 'no-cache',
        'Priority': 'u=0, i',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Sec-GPC': '1',
        'TE': 'trailers',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'#Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15'
    }
    return headers

class InvalidResponseException(Exception):
    pass

class NotFoundException(Exception):
    pass

class ProcessVideo:
    def __init__(self, r):
        self.r = r
        if r.status_code != 200:
            raise InvalidResponseException(
                r, f"TikTok returned a {r.status_code} status code."
            )
        self.text = ""
        self.start = -1
        self.json_start = '"webapp.video-detail":'
        self.json_start_len = len(self.json_start)
        self.end = -1
        self.json_end = ',"webapp.a-b":'
    
    def process_chunk(self, text_chunk):
        self.text += text_chunk
        if len(self.text) < self.json_start_len:
            return 'continue'
        if self.start == -1:
            self.start = self.text.find(self.json_start)
            if self.start != -1:
                self.text = self.text[self.start + self.json_start_len:]
                self.start = 0
        if self.start != -1:
            self.end = self.text.find(self.json_end)
            if self.end != -1:
                self.text = self.text[:self.end]
                return 'break'
        return 'continue'
            
    def process_response(self):
        if self.start == -1 or self.end == -1:
            raise InvalidResponseException(
                "Could not find normal JSON section in returned HTML.",
                json.dumps({'text': self.text, 'encoding': self.r.encoding}),
            )
        video_detail = json.loads(self.text)
        if video_detail.get("statusCode", 0) != 0: # assume 0 if not present
            # TODO retry when status indicates server error
            return video_detail
        video_info = video_detail.get("itemInfo", {}).get("itemStruct")
        if video_info is None:
            raise InvalidResponseException(
                video_detail, "TikTok JSON did not contain expected JSON."
            )
        return video_info

async def pytok_bytes(api, videos, logger, headless, request_delay):
    video_bytes = {}
    for video_data in videos:
        try:
            video_id = video_data['id']
            video = api.video(id=video_id)
            video_info = await video.info()
            video_video_bytes = await video.bytes()
            if video_video_bytes is not None:
                video_bytes[video_data['id']] = video_video_bytes
        except Exception as ex:
            logger.error(f"Error getting video bytes for {video_data['id']}: {ex}")
            continue

        # try:
        #     video_id = video_data['id']
        #     url = f"https://www.tiktok.com/@{video_data['author']['uniqueId']}/video/{video_id}"
        #     headers = get_headers()
            
        #     info_res = requests.get(url, headers=headers)
        #     video_processor = ProcessVideo(info_res)
        #     text_chunk = info_res.text
        #     if len(text_chunk) == 0:
        #         continue
        #     do = video_processor.process_chunk(text_chunk)

        #     bytes_headers = {
        #         'sec-ch-ua': '"HeadlessChrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"', 
        #         'referer': 'https://www.tiktok.com/', 
        #         'accept-encoding': 'identity;q=1, *;q=0', 
        #         'sec-ch-ua-mobile': '?0', 
        #         'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.6312.4 Safari/537.36', 
        #         'range': 'bytes=0-', 
        #         'sec-ch-ua-platform': '"Windows"'
        #     }

        #     video_d = video_processor.process_response()

        #     if 'video' not in video_d:
        #         continue

        #     cookies = {c.name: c.value for c in info_res.cookies}
        #     download_url = video_d['video']['downloadAddr'] if video_d['video']['downloadAddr'] else video_d['video']['playAddr']
        #     bytes_res = requests.get(download_url, headers=bytes_headers, cookies=cookies)
        #     if 200 <= bytes_res.status_code < 300:
        #         video_bytes[video_data['id']] = bytes_res.content
        # except Exception as ex:
        #     continue

    return video_bytes

class VideoBytesScraper:
    def __init__(self, logger, data_dir_path, headless=True, request_delay=3):
        self.logger = logger
        self.headless = headless
        self.data_dir_path = data_dir_path
        self.request_delay = request_delay

    async def get_video_bytes_batch(self, api, videos):
        
        try:
            # if self.lib == 'tiktokapi':
            #     video_bytes = await tiktokapi_bytes(videos, self.logger, self.request_delay)
            # elif self.lib == 'pytok':
            video_bytes = await pytok_bytes(api, videos, self.logger, self.headless, self.request_delay)

        except Exception as e:
            self.logger.error(f"Error getting batch: {e}")
            self.logger.error(f"Trackback: {traceback.format_exc()}")

        return video_bytes

    def save_data(self, data):
        video_bytes = data
        for video_id, byte_data in video_bytes.items():
            bytes_file = f"{video_id}.mp4"
            bytes_file_path = os.path.join(self.data_dir_path, bytes_file)
            with open(bytes_file_path, 'wb') as f:
                f.write(byte_data)
    

async def get_tiktok_video_bytes():
    data_dir_path = './data/mp4s'
    os.makedirs(data_dir_path, exist_ok=True)
    headless = False
    request_delay = 1
    batch_delay = 1

    scraper = VideoBytesScraper(
        logger, 
        data_dir_path, 
        headless=headless,
        request_delay=request_delay
    )

    saved_video_files = os.listdir(data_dir_path)
    saved_video_df = pl.DataFrame({'filename': saved_video_files})
    saved_video_df = saved_video_df.select(pl.col('filename').str.split('.').list.get(0).alias('id'))

    logger.info("Getting video df")
    video_path = f'./data/dubois_videos_backup.parquet.zstd'
    video_df = pl.read_parquet(video_path)

    video_df = video_df.join(saved_video_df, on='id', how='anti')
    video_df = video_df.filter(pl.col('video').struct.field('duration') > 0)
    # scrape video bytes

    batch_size = 20

    logger.info("Starting video bytes scrape")
    pbar = tqdm.tqdm(desc="Getting video bytes")
    for batch_i in range(0, len(video_df), batch_size):
        async with PyTok(manual_captcha_solves=False, headless=headless, logging_level=logging.DEBUG) as api:
            for video_data in video_df.slice(batch_i, batch_i + batch_size).to_dicts():
                pbar.update(1)
                try:
                    data = await scraper.get_video_bytes_batch(api, [video_data])
                    # waiting to ensure the data is saved before moving on
                    scraper.save_data(data)
                except Exception as ex:
                    continue

                # sleep to avoid rate limiting
                await asyncio.sleep(batch_delay)

    pbar.close()

def main():
    asyncio.run(get_tiktok_video_bytes())

if __name__ == '__main__':
    main()