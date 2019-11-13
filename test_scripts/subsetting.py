from google.cloud import bigquery
from urllib.parse import urlparse
import sqlite3
import sys
import time

def getData(datasetID, tableID, writeMode, queryStr):
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig()
    table_ref = client.dataset(datasetID).table(tableID)
    job_config.destination = table_ref
    job_config.allow_large_results = True
    job_config.write_disposition = writeMode
    
    query = (
        queryStr
    )
    query_job = client.query (
        query,
        location="US",
        job_config = job_config
    )

    return query_job

def populateTables(query_job):
    db_batch = 100
    i = 0
    sqdb = sqlite3.connect(sys.argv[1])
    sqcur = sqdb.cursor()

    for datapoint in query_job:
        # The following is for the crawl_data table in the schema
        load_id = datapoint.requestid
        scan_id = datapoint.pageid
        load_url = datapoint.url
        parsed_url = urlparse(load_url)
        if (len(parsed_url.netloc)) == 0:
            continue

        scheme = parsed_url.scheme
        port = None
        if scheme == "http":
            port = 80
        elif scheme == "https":
            port = 443

        load_domain = None
        if scheme.startswith("http"):
            # ignore other schemes, particularly data
            netloc = parsed_url.netloc
            try:
                load_domain, port = netloc.split(':')
                port = int(port)
            except:
                load_domain = netloc
        
        load_type = datapoint.mimeType

        sqcur.execute("insert or ignore into crawl_data values (?,?,?,?,?,?,?)", [load_id, scan_id, load_domain, scheme,
            port, load_url, load_type])

        # The following is for sites table in the schema
        # use scan_id as id
        site = datapoint.page
        parsed_site = urlparse(site)
        site_scheme = parsed_site.scheme
        site_domain = None
        if site_scheme.startswith("http"):
            # ignore other schemes, particularly data
            netloc = parsed_site.netloc
            try:
                site_domain, _ = netloc.split(':')
            except:
                site_domain = netloc
        cdn = datapoint._cdn_provider

        sqcur.execute("insert or ignore into sites values (?,?,?,?,?)", [scan_id, site, site_domain, site_scheme, int(time.time())])
        sqcur.execute("insert or ignore into load_cdn values (?,?,?,?)", [load_id, scan_id, load_url, cdn])
        
        i += 1
        if i % db_batch == 0:
            print('progress: %d ' % i)
            sqdb.commit()
    
if __name__ == "__main__":
    
    if len(sys.argv) < 2:
        print("Input the database name as an argument")
        exit()

    '''
    # OLDER QUERY--->
    query = (
        "SELECT requestid, pageid, mimeType, response_bodies.url, _cdn_provider, type, page "
        "FROM httparchive.almanac.summary_requests  AS response_bodies "
        "RIGHT JOIN ipprivacy.subsetting.2019_07_01_desktop_random AS random_urls "
        "ON response_bodies.page = random_urls.url"
    )
    '''
    queryStr = "SELECT requestid, pageid, url, NET.REG_DOMAIN(url) AS load_domain, page, NET.REG_DOMAIN(page) AS site_domain, _cdn_provider, type, mimeType FROM httparchive.almanac.summary_requests"
    query_job = getData("subsetting", "2019_07_01_desktop_postJoin2", 'WRITE_APPEND', queryStr)
    populateTables(query_job)