#!/usr/bin/env python
# MassRDAP - developed by acidvegas (https://git.acid.vegas/massrdap)

import asyncio
import logging
import json
import re
import os, random
from datetime import datetime

import pandas as pd

# import dask.dataframe as pd
from DataRecorder import Recorder
from dbhelper import *

# try:
#     import aiofiles
# except ImportError:
#     raise ImportError('missing required aiofiles library (pip install aiofiles)')

try:
    import aiohttp
except ImportError:
    raise ImportError("missing required aiohttp library (pip install aiohttp)")
import asyncio
import aiohttp_socks

from loguru import logger
import ray

# Replace this with your actual test URL
test_url = "http://example.com"
# ray.shutdown()
# ray.init()


MAX_RETRIES = 3
INITIAL_DELAY = 1
MAX_DELAY = 10


# Global variable to store RDAP servers
RDAP_SERVERS = {}


from bs4 import BeautifulSoup
import asyncio
import aiohttp
import time

# Semaphore to control concurrency
semaphore = asyncio.Semaphore(200)  # Allow up to 50 concurrent tasks
# db_manager = DatabaseManager()

filename = "majestic_million"
filename = "toolify.ai-organic-competitors--"
filename = "cftopai"
# filename='toolify-top500'
# filename='cloudflare-radar-domains-top-500000-20240617-20240624'
# filename='top-domains-1m'
# filename='character.ai-organic-competitors--'
# filename='efficient.app-organic-competitors--'
# filename='builtwith-top'
# filename='builtwith-top1m-20240621'
# filename='cloudflare-radar-domains-top-1000000-20240701-20240708'
# filename='top-1m-tranco'
# filename='top-1m-umbrella'
# filename='ahref-top'
filename = "./tranco_Z377G"
filename='domain-empty-intheirownwords'

folder_path = "."
inputfilepath = filename + ".csv"
# logger.add(f"{folder_path}/domain-index-ai.log")
# print(domains)

outfilepath = inputfilepath.replace(".csv", "-in.csv")
outfilepath = "top-domain-empty-intheirownwords-in.csv"

outfile = Recorder(folder_path + "/" + outfilepath, cache_size=200)
outfileerror = Recorder(
    folder_path + "/" + outfilepath.replace(".csv", "-error.csv"), cache_size=10
)


import mysql.connector
from mysql.connector import Error

# Database connection details
db_config = {
    'host': "gateway01.us-west-2.prod.aws.tidbcloud.com",
    'port': 4000,
    'user': "3i7meP2hYPkDk3V.root",
    'password': "bQnFW8QY5ALldnb6",
    'database': "test",
    'ssl_ca': "./isrgrootx1.pem",
    'ssl_verify_cert': True,
    'ssl_verify_identity': True
}

def insert_or_update_data(data_dict):
    try:
        # Create a connection to MySQL
        connection = mysql.connector.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database'],
            ssl_ca=db_config['ssl_ca'],
            ssl_verify_cert=db_config['ssl_verify_cert'],
            ssl_verify_identity=db_config['ssl_verify_identity']
        )
        
        if connection.is_connected():
            cursor = connection.cursor()

            # Prepare data from the dictionary
            domain = data_dict.get('domain')
            indexdate = data_dict.get('indexdate')
            aboutthesource = data_dict.get('Aboutthesource')
            intheirownwords = data_dict.get('Intheirownwords')

            if domain is not None:
                # Check if domain already exists
                check_query = "SELECT `indexdate`, `Aboutthesource`, `Intheirownwords` FROM `domain_index_data` WHERE `domain` = %s"
                cursor.execute(check_query, (domain,))
                existing_record = cursor.fetchone()

                if existing_record:
                    old_indexdate, old_aboutthesource, old_intheirownwords = existing_record

                    # Prepare update fields
                    update_fields = []
                    update_values = []
                    
                    if indexdate and (old_indexdate is None or old_indexdate == ''):
                        update_fields.append("`indexdate` = %s")
                        update_values.append(indexdate)
                    if aboutthesource and (old_aboutthesource is None or old_aboutthesource == ''):
                        update_fields.append("`Aboutthesource` = %s")
                        update_values.append(aboutthesource)
                    if intheirownwords and (old_intheirownwords is None or old_intheirownwords == ''):
                        update_fields.append("`Intheirownwords` = %s")
                        update_values.append(intheirownwords)
                    
                    if update_fields:
                        update_query = f"UPDATE `domain_index_data` SET {', '.join(update_fields)} WHERE `domain` = %s"
                        update_values.append(domain)
                        cursor.execute(update_query, tuple(update_values))
                        connection.commit()
                        print("Record updated successfully.")
                else:
                    # Insert new record if domain does not exist
                    insert_query = """
                        INSERT INTO `domain_index_data` (`domain`, `indexdate`, `Aboutthesource`, `Intheirownwords`)
                        VALUES (%s, %s, %s, %s)
                    """
                    cursor.execute(insert_query, (domain, indexdate, aboutthesource, intheirownwords))
                    connection.commit()
                    print("Data inserted successfully.")
            else:
                print("Invalid data: 'domain' is required.")
    
    except Error as e:
        print(f"An error occurred: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Example usage
data1 = {
    'domain': 'netnod.se',
    'indexdate': '2024-08-07',
    'Aboutthesource': 'More than 10 years ago',
    'Intheirownwords': 'xxx,zzz'
}






async def get_proxy():
    proxy = None
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get("http://demo.spiderpy.cn/get") as response:
                data = await response.json()
                proxy = data["proxy"]
                return proxy
        except:
            pass


async def get_proxy_proxypool():
    async with aiohttp.ClientSession() as session:

        try:
            async with session.get(
                "https://proxypool.scrape.center/random"
            ) as response:
                proxy = await response.text()
                return proxy
        except:
            return None





def get_tld(domain: str):
    """Extracts the top-level domain from a domain name."""
    parts = domain.split(".")
    return ".".join(parts[1:]) if len(parts) > 1 else parts[0]


# Function to extract data from HTTP response
async def extract_data(response):
    try:
        data = await response.json()
        return data
    except aiohttp.ContentTypeError:
        return None


async def extract_indedate(response, domain):
    try:
        date = "unk"
        url = f"https://www.google.com/search?q=About+{domain}&tbm=ilp&sa=X&ved=2ahUKEwj30IH2gZuHAxUPAkQIHb8sA_sQv5AHegQIABAD"

        data = await response.text()

        soup = BeautifulSoup(data, "html.parser")

        # Find all elements that contain the text 'aaa'

        elements_with_aaa = soup.find_all(
            lambda tag: "Site first indexed by Google" in tag.get_text()
        )
        logger.info("rawdata", data)

        # # Output the text content of each element that contains 'aaa'
        # for element in elements_with_aaa:
        data1 = ""
        data2 = ""
        if len(elements_with_aaa) > 0:
            indexdata = elements_with_aaa[0].get_text()
            print("site index data", indexdata)
            if "Web results about the source" in indexdata:
                r = indexdata.split("Web results about the source")[0]
                if "About the source" in r:
                    r = r.split("About the source")[-1]
                    if "In their own words" in r:
                        data1 = r.split("In their own words")[0]
                        # data1 = data1.split("\r")
                        data2 = r.split("In their own words")[-1]
                        data2 = data2.replace("\r", "")
                        data2 = data2.replace("\n", "")
            print("=", "Site first indexed by Google" in indexdata)
            print("0", "first indexed by Google" in indexdata)
            if "Site first indexed by Google" in indexdata:
                r = indexdata.split("Site first indexed by Google")
                # print("get data", r[0])
                print("get data", r[-1])
                date = r[-1]

                if date and not date.endswith("ago"):
                    date = date.split("ago")[0] + "ago"
        data = {
            "id": "111",
            "domain": domain,
            "indexdate": date,
            "Aboutthesource": data1,
            "Intheirownwords": data2,
        }
        outfile.add_data(data)
        data.pop('id')
        # insert_or_update_data(data)


        return True
    except Exception as e:
        logger.error(f"parse index date error for:{e}")
        # Domain=
        # new_domain = db_manager.Domain(
        #     url=domain,tld=get_tld(domain),
        # title=None,
        # indexat=r[-1] or None,
        # des=None,
        # bornat=None)
        # db_manager.add_domain(new_domain)
        return False
# Function to simulate a task asynchronously
async def fetch_data(url, valid_proxies=None, data_format="json"):
    async with semaphore:

        retries = 4
        for attempt in range(1, retries + 1):
            try:
                proxy_url = None  # Example SOCKS5 proxy URL
                if attempt == 3:
                    if valid_proxies:
                        proxy_url = random.choice(valid_proxies)
                elif attempt == 2:
                    # proxy_url=await get_proxy_proxypool()
                    proxy_url = "socks5://127.0.0.1:1080"  # Example SOCKS5 proxy URL
                elif attempt == 4:
                    proxy_url = await get_proxy()
                # proxy_url = "socks5://127.0.0.1:9050"  # Example SOCKS5 proxy URL
                connector = (
                    aiohttp_socks.ProxyConnector.from_url(proxy_url)
                    if proxy_url and proxy_url.startswith("socks")
                    else None
                )
                proxy = proxy_url if proxy_url and "http" in proxy_url else None
                print("===proxy", proxy, url)
                async with aiohttp.ClientSession(connector=connector) as session:
                    async with session.get(url, proxy=proxy) as response:
                        if response.status == 200:
                            # data = await extract_indedate(response, domain)
                            # print('data',data)
                            print(f"Task {url} completed on attempt {attempt}.")
                            return (
                                await response.json()
                                if data_format == "json"
                                else await response.text()
                            )
                        else:
                            print(
                                f"Task {url} failed on attempt {attempt}. Status code: {response.status}"
                            )
            except aiohttp.ClientConnectionError:
                if attempt < retries:
                    print(f"Task {url} failed on attempt {attempt}. Retrying...")
                else:
                    print(f"Task {url} failed on all {retries} attempts. Skipping.")
                    # outfileerror.add_data([domain])

            except Exception:
                if attempt < retries:
                    print(f"Task {url} failed on attempt {attempt}. Retrying...")
                else:
                    print(f"Task {url} failed on all {retries} attempts. Skipping.")
                    # outfileerror.add_data([domain])



# Function to simulate a task asynchronously
async def get_index_date(domain):
    async with semaphore:
        url = f"https://www.google.com/search?q=About+{domain}&tbm=ilp&sa=X&ved=2ahUKEwj30IH2gZuHAxUPAkQIHb8sA_sQv5AHegQIABAD"

        retries = 3
        for attempt in range(1, retries + 1):
            try:
                proxy_url = "socks5://127.0.0.1:1080"  # Example SOCKS5 proxy URL
                if attempt > 1:
                    # proxy_url=await get_proxy_proxypool()
                    proxy_url = "socks5://127.0.0.1:1080"  # Example SOCKS5 proxy URL

                # proxy_url = "socks5://127.0.0.1:9050"  # Example SOCKS5 proxy URL
                connector = (
                    aiohttp_socks.ProxyConnector.from_url(proxy_url)
                    if proxy_url and proxy_url.startswith("socks")
                    else None
                )
                proxy = proxy_url if proxy_url and "http" in proxy_url else None
                print("===proxy", proxy, domain)
                async with aiohttp.ClientSession(connector=connector) as session:
                    async with session.get(url, proxy=proxy) as response:
                        if response.status == 200:
                            data = await extract_indedate(response, domain)
                            # print('data',data)
                            if data:
                                print(
                                    f"Task {url} completed on attempt {attempt}. Data: {data}"
                                )
                                return
                        else:
                            print(
                                f"Task {url} failed on attempt {attempt}. Status code: {response.status}"
                            )
            except aiohttp.ClientConnectionError:
                if attempt < retries:
                    print(f"Task {url} failed on attempt {attempt}. Retrying...")
                else:
                    print(f"Task {url} failed on all {retries} attempts. Skipping.")
                    outfileerror.add_data([domain])

            except Exception:
                if attempt < retries:
                    print(f"Task {url} failed on attempt {attempt}. Retrying...")
                else:
                    print(f"Task {url} failed on all {retries} attempts. Skipping.")
                    outfileerror.add_data([domain])


# To run the async function, you would do the following in your main code or script:
# asyncio.run(test_proxy('your_proxy_url_here'))
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


# Function to run tasks asynchronously with specific concurrency
async def run_async_tasks():
    tasks = []

    df = pd.read_csv(
        inputfilepath,
        #  , encoding="ISO-8859-1"
        usecols=["domain"],
    )
    domains = df["domain"].to_list()
    print(f"load domains:{len(domains)}")
    domains = [cleandomain(element) for element in set(domains)]

    try:
        # dbdata=db_manager.read_domain_all()

        # for i in dbdata:
        #     if i.indexat is not None:
        #         donedomains.append(i.url)
        pass
    except Exception as e:
        print(f"query error: {e}")

    # 初始化一个空列表来收集所有域
    alldonedomains = []
    if os.path.exists(outfilepath):
    # df=pd.read_csv(outfilepath)
    # filtered_df = df[df['indexdate'] != 'unk']
    # print(df.head(50))
    # alldonedomains=df['domain'].to_list()
    # else:
    #     df=pd.read_csv('top-domains-1m.csv')

    #     donedomains=df['domain'].to_list()
    # donedomains=list(set(donedomains))

        # 使用chunksize读取数据，返回一个可迭代的TextFileReader对象
        chunk_iter = pd.read_csv(outfilepath, chunksize=100000)


        # 逐个处理每个数据块
        for chunk in chunk_iter:
            # 将当前块的'domain'列转换为列表并添加到all_done_domains中
            alldonedomains.extend(chunk["domain"].dropna().unique().tolist())

    alldonedomains = set(alldonedomains)

    print(f"load alldonedomains:{len(list(alldonedomains))}")

    donedomains = [element for element in domains if element in alldonedomains]

    print(f"load done domains {len(donedomains)}")

    tododomains = list(set(domains) - set(donedomains))

    print(f"to be done {len(tododomains)}")
    time.sleep(120)
    for domain in tododomains:

        domain = cleandomain(domain)
        # print(domain)
        if domain not in donedomains:
            print("add domain", domain)
            task = asyncio.create_task(get_index_date(domain))
            tasks.append(task)
            if len(tasks) >= 100:
                # Wait for the current batch of tasks to complete
                await asyncio.gather(*tasks)
                tasks = []
    await asyncio.gather(*tasks)


# Example usage: Main coroutine
async def main():
    start_time = time.time()
    await run_async_tasks()
    print(
        f"Time taken for asynchronous execution with concurrency limited by semaphore: {time.time() - start_time} seconds"
    )


# Manually manage the event loop in Jupyter Notebook or other environments
if __name__ == "__main__":
    # logger.add('google-index-debug.log')

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
    outfile.record()
    outfileerror.record()
    ray.shutdown()
