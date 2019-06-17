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
        "SELECT pages.pageid as id, pages.url as siteURL, requests.url as requestURL FROM httparchive.summary_pages." + table_num + " pages INNER JOIN httparchive.summary_requests." + table_num + " requests ON pages.pageid = requests.pageid LIMIT 10 "
    )
    query_job = client.query(
        query,
        location="US",
    )
    
    i = 0 # get last index from the db
    try:
        for row in sqcur.execute("select id from bq_crawl order by id desc limit 1"):
            i = row[0] + 1
    except:
        i = 0
    
    print("starting job with i as ", i)
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
                    flag = True
                    break
            if flag:
                continue
            if (len(parsed_url.netloc)) == 0:
                continue

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
            try:
                resource = r.headers['Content-Type'].split('/')[0]
            except:
                resource = "Content-Type Absent"
            
            sqcur.execute("insert or ignore into sites values (?,?,?,?,?)", [load_id, site, load_domain,
                load_scheme, int(time.time())])
            sqcur.execute("insert or ignore into bq_crawl values (?,?,?,?,?,?,?,?)", [i, site, load_domain,url,
                load_scheme, load_port, int(time.time()),resource])
        
            i += 1
            if i % DB_BATCH == 0:
                print('progress: %d ' % i)
                sqdb.commit()
        except Exception as e:
            print("Exception: ", e)
            exc_type, _, exc_tb = sys.exc_info()
            print(exc_type, exc_tb.tb_lineno, "\n\n")
    sqdb.commit()

def main():
    with open("tables") as table_file:
        for table_num in table_file:
            print("TABLE: ", table_num.strip('\n'))
            generate_data(table_num.strip('\n'))

if __name__ == '__main__':
    main()