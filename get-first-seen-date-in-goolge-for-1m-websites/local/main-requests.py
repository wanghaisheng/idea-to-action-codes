#!/usr/bin/env python
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import csv
import os
import time
from dbhelper import load_undone_domains_csv, load_undone_domains_mysql, load_undone_domains_cloudflare, save_data_csv, save_data_mysql, save_data_cloudflare
from concurrent.futures import ThreadPoolExecutor, as_completed

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

def extract_indedate(response, domain):
    try:
        date = "unk"
        soup = BeautifulSoup(response.text, "html.parser")
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

def get_index_date(domain):
    retries = 3
    url = f"https://www.google.com/search?q=About+{domain}&tbm=ilp&sa=X&ved=2ahUKEwj30IH2gZuHAxUPAkQIHb8sA_sQv5AHegQIABAD"
    
    session = requests.Session()
    retry = Retry(connect=retries, backoff_factor=0.3)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    for attempt in range(1, retries + 1):
        try:
            response = session.get(url)
            if response.status_code == 200:
                success = extract_indedate(response, domain)
                if success:
                    print(f"Task {url} completed on attempt {attempt}.")
                    return
            else:
                print(f"Task {url} failed on attempt {attempt}. Status code: {response.status_code}")
        except requests.RequestException as e:
            print(f"Task {url} failed on attempt {attempt}. Error: {e}")
            if attempt >= retries:
                outfileerror.add_data({"domain": domain, "error": str(e)})

def run_tasks():
    all_domains = set(load_undone_domains_csv('path_to_csv') + 
                      load_undone_domains_mysql() + 
                      load_undone_domains_cloudflare())

    with ThreadPoolExecutor(max_workers=200) as executor:
        future_to_domain = {executor.submit(get_index_date, cleandomain(domain)): domain for domain in all_domains}
        for future in as_completed(future_to_domain):
            domain = future_to_domain[future]
            try:
                future.result()
            except Exception as exc:
                print(f"Task for {domain} generated an exception: {exc}")

def main():
    start_time = time.time()
    run_tasks()
    outfile.record()
    outfileerror.record()
    print(f"Time taken: {time.time() - start_time} seconds")

if __name__ == "__main__":
    main()
