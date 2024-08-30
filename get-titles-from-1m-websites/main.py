#!/usr/bin/env python

import asyncio
import logging
import json
import re
import os
import random
import time
from datetime import datetime

import pandas as pd
from aiohttp import ClientSession, ClientConnectionError
from aiohttp_socks import ProxyConnector
from bs4 import BeautifulSoup
from loguru import logger

from DataRecorder import Recorder
from dbhelper import add_domain, Domain

# Configuration
class Config:
    TEST_URL = "http://example.com"
    INPUT_FILENAME = "domain-ai-in-name"
    FOLDER_PATH = "."
    MAX_RETRIES = 3
    INITIAL_DELAY = 1
    MAX_DELAY = 10
    SEMAPHORE_LIMIT = 100
    PROXY_POOL_URL = 'https://proxypool.scrape.center/random?https'
    PROXY_FILE_PATH = r'D:\Download\audio-visual\a_proxy_Tool\proxy-scraper-checker\out-google\proxies'

# Logger setup
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
logger.add(f"{Config.FOLDER_PATH}/domain-index-ai.log")

class DomainProcessor:
    def __init__(self):
        self.semaphore = asyncio.Semaphore(Config.SEMAPHORE_LIMIT)
        self.outfile = Recorder(os.path.join(Config.FOLDER_PATH, f"{Config.INPUT_FILENAME}-title.csv"), cache_size=50)
        self.valid_proxies = self.get_local_proxies()
        self.already_done_domains = self.load_done_domains()

    def get_local_proxies(self):
        raw_proxies = []
        for p in ['http', 'socks4', 'socks5']:
            proxyfile = os.path.join(Config.PROXY_FILE_PATH, f'{p}.txt')
            if os.path.exists(proxyfile):
                with open(proxyfile, "r", encoding="utf8") as file:
                    raw_proxies.extend([f'{p}://{v.strip()}' for v in file if v.strip()])
        return list(set(raw_proxies))

    def load_done_domains(self):
        if os.path.exists(self.outfile.file_path):
            df = pd.read_csv(self.outfile.file_path)
            return set(df['domain'].tolist())
        return set()

    async def fetch_data(self, session, url, proxy):
        async with session.get(url, proxy=proxy) as response:
            if response.status == 200:
                return await response.text()
            logger.warning(f"Failed to fetch {url}. Status code: {response.status}")
            return None

    async def extract_title_des(self, response, domain):
        try:
            html = await response.text()
            title = self.get_title_from_html(html)
            des = self.get_des_from_html(html)
            raw = self.get_text_from_html(html)
            lang = self.detect_language(raw) if raw else "Unknown"
            data = {
                'domain': domain,
                "title": title,
                'des': des,
                'raw': raw,
                'lang': lang
            }
            self.outfile.add_data(data)
            return True
        except Exception as e:
            logger.error(f"Error extracting data from {domain}: {e}")
            return False

    def get_title_from_html(self, html):
        match = re.search(r'<title>(.*?)</title>', html, re.IGNORECASE)
        return match.group(1).strip() if match else 'not content!'

    def get_des_from_html(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        meta_tag = soup.find('meta', attrs={'name': 'description'})
        return meta_tag['content'].strip() if meta_tag else 'No description found'

    def get_text_from_html(self, html):
        import trafilatura
        return trafilatura.extract(html)

    def detect_language(self, text):
        import py3langid as langid
        return langid.classify(text)[0] if text else "Unknown"

    async def process_domain(self, domain):
        url = f'https://{domain}'
        for attempt in range(1, Config.MAX_RETRIES + 1):
            try:
                proxy = random.choice(self.valid_proxies) if attempt == 1 else None
                connector = ProxyConnector.from_url(proxy) if proxy and proxy.startswith("socks") else None
                async with ClientSession(connector=connector) as session:
                    response_text = await self.fetch_data(session, url, proxy)
                    if response_text:
                        success = await self.extract_title_des(response_text, domain)
                        if success:
                            logger.info(f"Successfully processed {domain} on attempt {attempt}")
                            return
            except (ClientConnectionError, asyncio.CancelledError) as e:
                logger.warning(f"Attempt {attempt} failed for {domain}: {e}")
            await asyncio.sleep(Config.INITIAL_DELAY * attempt)

    async def run(self):
        df = pd.read_csv(f"{Config.INPUT_FILENAME}.csv", encoding="ISO-8859-1")
        domains = set(df['domain'].tolist())
        todo_domains = domains - self.already_done_domains
        tasks = []
        for domain in todo_domains:
            task = asyncio.create_task(self.process_domain(domain))
            tasks.append(task)
            if len(tasks) >= Config.SEMAPHORE_LIMIT:
                await asyncio.gather(*tasks)
                tasks = []
        await asyncio.gather(*tasks)

async def main():
    start_time = time.time()
    processor = DomainProcessor()
    await processor.run()
    logger.info(f"Execution time: {time.time() - start_time:.2f} seconds")

if __name__ == "__main__":
    asyncio.run(main())
