#!/usr/bin/env python
import asyncio
import logging
import csv
import os
import pandas as pd
import aiohttp
import aiohttp_socks
import time
from bs4 import BeautifulSoup
from dbhelper import load_undone_domains_csv, load_undone_domains_mysql, load_undone_domains_cloudflare, save_data_csv, save_data_mysql, save_data_cloudflare

# Global semaphore for concurrency control
semaphore = asyncio.Semaphore(200)

# Filepaths
inputfilepath = "domains.csv"
outfilepath = "output.csv"
errorfilepath = "errors.csv"

# Initialize data recorders
class Recorder:
    def __init__(self, filepath, cache_size=100):
        self.filepath = filepath
        self.cache_size = cache_size
        self.cache = []
    
    def add_data(self, data):
        self.cache.append(data)
        if len(self.cache) >= self.cache_size:
            self.save_data()
    
    def save_data(self):
        if not self.cache:
            return
        save_data_csv(self.filepath, self.cache)
        self.cache = []
    
    def record(self):
        self.save_data()

outfile = Recorder(outfilepath, cache_size=200)
outfileerror = Recorder(errorfilepath, cache_size=10)

def cleandomain(domain):
    if isinstance(domain, str) == False:
        domain = str(domain)
    domain = domain.strip()
    if "https://" in domain:
        domain = domain.replace("https://", "")
    if "http://" in domain:
        domain = domain.replace("http://", "")
    if "www." in domain:
        domain = domain.replace("www.", "")
    if domain.endswith("/"):
        domain = domain.rstrip("/")
    return domain

async def extract_data(response):
    try:
        return await response.json()
    except aiohttp.ContentTypeError:
        return None

async def extract_indedate(response, domain):
    try:
        date = "unk"
        url = f"https://www.google.com/search?q=About+{domain}&tbm=ilp&sa=X&ved=2ahUKEwj30IH2gZuHAxUPAkQIHb8sA_sQv5AHegQIABAD"
        data = await response.text()
        soup = BeautifulSoup(data, "html.parser")
        elements_with_aaa = soup.find_all(
            lambda tag: "Site first indexed by Google" in tag.get_text()
        )

        data1 = ""
        data2 = ""
        if elements_with_aaa:
            indexdata = elements_with_aaa[0].get_text()
            if "Web results about the source" in indexdata:
                r = indexdata.split("Web results about the source")[0]
                if "About the source" in r:
                    r = r.split("About the source")[-1]
                    if "In their own words" in r:
                        data1 = r.split("In their own words")[0]
                        data2 = r.split("In their own words")[-1]
                        data2 = data2.replace("\r", "").replace("\n", "")
            if "Site first indexed by Google" in indexdata:
                r = indexdata.split("Site first indexed by Google")
                date = r[-1]
                if date and not date.endswith("ago"):
                    date = date.split("ago")[0] + "ago"
        
        data = {
            "domain": domain,
            "indexdate": date,
            "Aboutthesource": data1,
            "Intheirownwords": data2,
        }
        outfile.add_data(data)
        data.pop('id')
        return True
    except Exception as e:
        print(f"Parse index date error for: {e}")
        outfileerror.add_data({"domain": domain, "error": str(e)})
        return False

async def get_index_date(domain):
    async with semaphore:
        url = f"https://www.google.com/search?q=About+{domain}&tbm=ilp&sa=X&ved=2ahUKEwj30IH2gZuHAxUPAkQIHb8sA_sQv5AHegQIABAD"
        retries = 3
        for attempt in range(1, retries + 1):
            try:
                proxy_url = "socks5://127.0.0.1:1080"
                connector = (
                    aiohttp_socks.ProxyConnector.from_url(proxy_url)
                    if proxy_url and proxy_url.startswith("socks")
                    else None
                )
                async with aiohttp.ClientSession(connector=connector) as session:
                    async with session.get(url, proxy=proxy_url) as response:
                        if response.status == 200:
                            data = await extract_indedate(response, domain)
                            if data:
                                print(f"Task {url} completed on attempt {attempt}. Data: {data}")
                                return
                        else:
                            print(f"Task {url} failed on attempt {attempt}. Status code: {response.status}")
            except aiohttp.ClientConnectionError:
                if attempt < retries:
                    print(f"Task {url} failed on attempt {attempt}. Retrying...")
                else:
                    print(f"Task {url} failed on all {retries} attempts. Skipping.")
                    outfileerror.add_data({"domain": domain, "error": "Connection error"})
            except Exception:
                if attempt < retries:
                    print(f"Task {url} failed on attempt {attempt}. Retrying...")
                else:
                    print(f"Task {url} failed on all {retries} attempts. Skipping.")
                    outfileerror.add_data({"domain": domain, "error": "General error"})

async def run_async_tasks():
    tasks = []

    # Load undone domains
    csv_domains = load_undone_domains_csv('path_to_csv')
    mysql_domains = load_undone_domains_mysql()
    cloudflare_domains = load_undone_domains
