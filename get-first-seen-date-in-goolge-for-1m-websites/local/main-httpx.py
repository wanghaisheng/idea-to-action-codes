import asyncio
import httpx
from httpx_socks import SOCKSProxy
import pandas as pd
from bs4 import BeautifulSoup
import time
import dbhelper  # Importing your dbhelper module

# Configuration
source = "csv"  # Can be "csv", "mysql", or "cloudflare"
proxy_url = "socks5://127.0.0.1:1080"

# Recorder class for managing output
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
        with open(self.filepath, 'a', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=self.cache[0].keys())
            if file.tell() == 0:
                writer.writeheader()
            writer.writerows(self.cache)
        self.cache = []
    
    def record(self):
        self.save_data()

outfile = Recorder("output.csv", cache_size=200)
outfileerror = Recorder("errors.csv", cache_size=10)

def cleandomain(domain):
    if not isinstance(domain, str):
        domain = str(domain)
    domain = domain.strip().replace("https://", "").replace("http://", "").replace("www.", "")
    return domain.rstrip("/")

async def extract_indedate(response, domain):
    try:
        soup = BeautifulSoup(response.text, "html.parser")
        elements_with_aaa = soup.find_all(
            lambda tag: "Site first indexed by Google" in tag.get_text()
        )

        data1 = ""
        data2 = ""
        date = "unk"
        if elements_with_aaa:
            indexdata = elements_with_aaa[0].get_text()
            if "Web results about the source" in indexdata:
                r = indexdata.split("Web results about the source")[0]
                if "About the source" in r:
                    r = r.split("About the source")[-1]
                    if "In their own words" in r:
                        data1 = r.split("In their own words")[0]
                        data2 = r.split("In their own words")[-1].replace("\r", "").replace("\n", "")
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
        return True
    except Exception as e:
        outfileerror.add_data({"domain": domain, "error": str(e)})
        return False

async def get_index_date(domain):
    retries = 3
    url = f"https://www.google.com/search?q=About+{domain}&tbm=ilp&sa=X&ved=2ahUKEwj30IH2gZuHAxUPAkQIHb8sA_sQv5AHegQIABAD"
    
    async with httpx.AsyncClient(proxies=proxy_url) as client:
        for attempt in range(1, retries + 1):
            try:
                response = await client.get(url)
                if response.status_code == 200:
                    success = await extract_indedate(response, domain)
                    if success:
                        print(f"Task {url} completed on attempt {attempt}.")
                        return
                else:
                    print(f"Task {url} failed on attempt {attempt}. Status code: {response.status_code}")
            except httpx.RequestError as e:
                print(f"Task {url} failed on attempt {attempt}. Error: {e}")
                if attempt >= retries:
                    outfileerror.add_data({"domain": domain, "error": str(e)})

async def run_async_tasks():
    # Load domains from the selected source
    if source == "csv":
        df = dbhelper.load_data_csv("domains.csv")
    elif source == "mysql":
        df = dbhelper.load_data_mysql()
    elif source == "cloudflare":
        df = dbhelper.load_data_cloudflare_d1()
    else:
        raise ValueError("Unknown source specified")

    domains = df["domain"].dropna().unique().tolist()
    domains = [cleandomain(domain) for domain in domains]
    
    tasks = [get_index_date(domain) for domain in domains]
    await asyncio.gather(*tasks)

async def main():
    start_time = time.time()
    await run_async_tasks()
    outfile.record()
    outfileerror.record()
    print(f"Time taken: {time.time() - start_time} seconds")

if __name__ == "__main__":
    asyncio.run(main())
