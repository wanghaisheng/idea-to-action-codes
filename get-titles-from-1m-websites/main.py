#!/usr/bin/env python

import asyncio
import logging
import re
import os
import random
import time
from bs4 import BeautifulSoup
import aiohttp
import aiohttp_socks
import pandas as pd
from dbhelper import MySQLHelper, D1Helper, Domain
from Recorder import Recorder  # Assuming you have a Recorder class defined elsewhere

# Configurations
class Config:
    SEMAPHORE_LIMIT = 100
    FOLDER_PATH = '.'
    INPUT_FILENAME = 'domain-ai-in-name'
    DB_TYPE = 'mysql'  # Change to 'd1' for Cloudflare D1
    MYSQL_CONFIG = {
        'host': 'localhost',
        'user': 'root',
        'password': 'password',
        'database': 'mydatabase'
    }
    D1_CONFIG = {
        'api_token': 'your_cloudflare_api_token',
        'database_id': 'your_database_id'
    }

# Logging Setup
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Helper Functions
def get_tld(domain: str):
    """Extracts the top-level domain from a domain name."""
    parts = domain.split(".")
    return ".".join(parts[1:]) if len(parts) > 1 else parts[0]

def get_title_from_html(html):
    title = 'not content!'
    try:
        title_patten = r'<title>(\s*?.*?\s*?)</title>'
        result = re.findall(title_patten, html)
        if len(result) >= 1:
            title = result[0].strip()
    except:
        logger.error('Cannot find title')
    return title

def get_des_from_html(html):
    description = 'not content!'
    try:
        soup = BeautifulSoup(html, 'html.parser')
        description_tag = soup.find('meta', attrs={'name': 'description'})
        description = description_tag['content'] if description_tag else 'No description found'
        description = description.replace('\n', '').replace('\r', '').strip()
        logger.info(f'Found description: {description}')
    except:
        logger.error('Cannot find description')
    return description

def get_text_from_html(html):
    import trafilatura
    return trafilatura.extract(html)

def detect_language(text):
    import py3langid as langid
    lang = langid.classify(text)
    return lang[0] if lang else "Unknown"

def cleandomain(domain):
    if isinstance(domain, str) is False:
        domain = str(domain)
    domain = domain.strip()
    domain = domain.replace("https://", "").replace("http://", "").replace("www.", "").rstrip("/")
    return domain

async def get_local_proxies():
    raw_proxies = []
    proxy_dir = r'D:\Download\audio-visual\a_proxy_Tool\proxy-scraper-checker\out-google\proxies'
    for p in ['http', 'socks4', 'socks5']:
        proxyfile = os.path.join(proxy_dir, f'{p}.txt')
        if os.path.exists(proxyfile):
            with open(proxyfile, "r", encoding="utf8") as file:
                lines = file.readlines()
                raw_proxies += [f'{p}://'+v.strip() for v in lines]
    raw_proxies = list(set(raw_proxies))
    logger.info(f'Raw proxies count: {len(raw_proxies)}')
    return raw_proxies

async def extract_title_des(response, domain, db_helper):
    try:
        html = await response.text()
        title = get_title_from_html(html)
        des = get_des_from_html(html)
        raw = get_text_from_html(html)
        lang = detect_language(raw) if raw else "Unknown"
        data = {
            'domain': domain,
            "title": title,
            'des': des,
            'raw': raw,
            'lang': lang
        }
        outfile.add_data(data)
        domain_data = Domain(domain, get_tld(domain), title, des, raw, lang)
        db_helper.add_domain(domain_data)
        return True
    except Exception as e:
        logger.error(f"Error extracting data from {domain}: {e}")
        return False

async def get_title_des(domain, valid_proxies, semaphore, db_helper):
    async with semaphore:
        url = f'https://{domain}'
        retries = 3
        for attempt in range(1, retries + 1):
            try:
                proxy_url = None
                if attempt == 1 and valid_proxies:
                    proxy_url = random.choice(valid_proxies)
                elif attempt == 2:
                    proxy_url = "http://127.0.0.1:1080"
                connector = aiohttp_socks.ProxyConnector.from_url(proxy_url) if proxy_url and proxy_url.startswith("socks") else None
                async with aiohttp.ClientSession(connector=connector) as session:
                    async with session.get(url, proxy=proxy_url) as response:
                        if response.status == 200:
                            await extract_title_des(response, domain, db_helper)
                            return
                        else:
                            logger.warning(f"Task {url} failed on attempt {attempt}. Status code: {response.status}")
            except Exception as e:
                if attempt < retries:
                    logger.warning(f"Task {url} failed on attempt {attempt}. Retrying... Error: {e}")
                else:
                    logger.error(f"Task {url} failed on all {retries} attempts. Skipping.")

async def run_async_tasks():
    tasks = []
    df = pd.read_csv(Config.INPUT_FILENAME + ".csv", encoding="ISO-8859-1")
    domains = df['domain'].to_list()
    logger.info('Loaded domains')

    valid_proxies = await get_local_proxies()
    db_helper = MySQLHelper(**Config.MYSQL_CONFIG) if Config.DB_TYPE == 'mysql' else D1Helper(**Config.D1_CONFIG)

    completed_domains = set()
    if os.path.exists(Config.INPUT_FILENAME + "-title.csv"):
        df = pd.read_csv(Config.INPUT_FILENAME + "-title.csv")
        completed_domains = set(df['domain'].to_list())
    
    pending_domains = list(set(domains) - completed_domains)
    logger.info(f'Domains to be processed: {len(pending_domains)}')

    semaphore = asyncio.Semaphore(Config.SEMAPHORE_LIMIT)
    for domain in pending_domains:
        domain = cleandomain(domain)
        task = asyncio.create_task(get_title_des(domain, valid_proxies, semaphore, db_helper))
        tasks.append(task)
        if len(tasks) >= Config.SEMAPHORE_LIMIT:
            await asyncio.gather(*tasks)
            tasks = []
    await asyncio.gather(*tasks)
    db_helper.close()

if __name__ == "__main__":
    asyncio.run(run_async_tasks())
