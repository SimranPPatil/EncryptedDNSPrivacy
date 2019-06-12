from google.cloud import bigquery
from urllib.parse import urlparse
from datetime import datetime
from difflib import SequenceMatcher
import multiprocessing as mp
from pymongo import MongoClient, CursorType
from collections import defaultdict
import json
import sqlite3
import sys
import time
import os
import requests
import csv

DB_BATCH = 10

if len(sys.argv) < 2:
    print("Input the database name as an argument")
    exit()

today = str(datetime.now()).split(' ')[0]
print("Run on: ", today)

# export GOOGLE_APPLICATION_CREDENTIALS="[PATH]"

def generate_data(table_num):
    sqdb = sqlite3.connect(sys.argv[1])
    sqcur = sqdb.cursor()

    client = bigquery.Client()
    query = (
        "SELECT pages.pageid as id, pages.url as siteURL, requests.url as requestURL FROM httparchive.summary_pages." + table_num + " pages INNER JOIN httparchive.summary_requests." + table_num + " requests ON pages.pageid = requests.pageid LIMIT 70 "
    )
    query_job = client.query(
        query,
        location="US",
    )
    sites_to_resources = {}
    sites_to_url = {}
    sites_to_id = {}
    i = 0
    for obj in query_job:
        try:
            load_id = obj["id"]
            url = obj["requestURL"]
            parsed_url = urlparse(url)
            site = obj['siteURL']
            # unique site url or a unique page load id corresponding to a site url
            # several request urls per site url
            start = time.time()
            TO = 10
            body = []
            flag = False
            r = requests.get(url.strip('\n'), timeout=(5,5), stream = True)
            for chunk in r.iter_content(1024):
                body.append(chunk)
                if time.time() > (start + TO):
                    print("body: ", len(body), "\n")
                    flag = True
                    break
            if flag:
                continue
            if (len(parsed_url.netloc)) == 0:
                continue
            try:
                resource = r.headers['Content-Type'].split('/')[0]
            except:
                resource = "Content-Type Absent"
            sqcur.execute("insert or ignore into sites values (?,?,?,?,?)", [load_id, site, parsed_url.netloc,
                parsed_url.scheme, int(time.time())])
            sqcur.execute("insert or ignore into bq_crawl values (?,?,?,?,?,?,?)", [load_id, site, parsed_url.netloc,url,
                parsed_url.scheme, int(time.time()),resource])
            sites_to_resources.setdefault(site, []).append(resource)
            sites_to_url.setdefault(site,[]).append(url)
            sites_to_id[site] = load_id

            i += 1
            if i % DB_BATCH == 0:
                print('metadata progress: %d ' % i)
                sqdb.commit()
        except Exception as e:
            print("Exception: ", e)
            exc_type, _, exc_tb = sys.exc_info()
            print(exc_type, exc_tb.tb_lineno, "\n\n")
    sqdb.commit()

    print('Finished reading metadata file, begin queries for resource data')

    total_sites = len(sites_to_resources)
    i = 0
    print("Checking sites that have been done")
    sqcur.execute("select distinct(scan_id) from crawl_data")
    for row in sqcur:
        try:
            sites_to_resources.pop(row[0])
        except Exception as e:
            print("Exception: ", e)
            exc_type, _, exc_tb = sys.exc_info()
            print(exc_type, exc_tb.tb_lineno, "\n\n")

    to_process = len(sites_to_resources)
    print(f"{to_process}/{total_sites} sites to do")

    for site in sites_to_resources:
        try:
            urls = sites_to_url[site]
            for url in urls:
                load_id = sites_to_id[site]
                parsed_url = urlparse(url)
                resource = sites_to_resources[site]

                load_scheme = parsed_url.scheme
                load_port = None
                if load_scheme == "http":
                    load_port = 80
                elif load_scheme == "https":
                    load_port = 443

                load_domain = None
                if load_scheme.startswith("http"):
                    # ignore other schemes, particularly data
                    netloc = parsed_url.netloc
                    try:
                        load_domain, load_port = netloc.split(':')
                        load_port = int(load_port)
                    except:
                        load_domain = netloc
                res = ",".join(resource)
                sqcur.execute("insert into crawl_data values (?,?,?,?,?,?,?)",
                            [load_id, site, load_domain, load_scheme, load_port, url,res])

                i += 1
                if i % DB_BATCH == 0:
                    # Track progress
                    print('Processed  %d / %d ' % (i, to_process))
                    sqdb.commit()
        except Exception as e:
            print("Exception: ", e, site)
            exc_type, _, exc_tb = sys.exc_info()
            print(exc_type, exc_tb.tb_lineno, "\n\n")
    sqdb.commit()
def main():
    with open("t2") as table_file:
        for table_num in table_file:
            print("TABLE: ", table_num.strip('\n'))
            generate_data(table_num.strip('\n'))

if __name__ == '__main__':
    main()