import os
import sqlite3
import sys
import json
import asyncio
from asyncio.subprocess import PIPE
import aiosqlite
import aiofiles
from os.path import expanduser
from google.cloud import storage
from google.cloud import bigquery
from google.cloud import bigquery_storage_v1beta1

ZDNS = expanduser("~/go/bin/zdns")
BATCH_SIZE = 100

async def batch_writer(file_in):
    async with aiofiles.open("temp.csv", "r") as csv:
        new_cnt = 0
        async for entry in csv:
            try:
                domain = entry.strip()
            except:
                domain = None
            print("domain: ", domain)

            file_in.write(f"{domain}\n".encode())
            await file_in.drain()
            new_cnt += 1
        print(f"Sent {new_cnt} items")
    file_in.write_eof()
    await file_in.drain()

async def batch_reader(file, rows_to_insert):
    counter = 0
    while True:
        line = await file.readline()
        if not line:
            break

        data = json.loads(line)
        domain = data['name']
        
        if 'data' in data and 'answers' in data['data'] and data['data']['answers']:
            for a in data['data']['answers']:
                print(domain, a)
                try:
                    rows_to_insert.append((domain, a['answer'].strip('.')))
                except:
                    rows_to_insert.append((domain, "NONE"))
        else:
            rows_to_insert.append((domain, "NONE"))
        counter += 1

    if len(rows_to_insert):
        print(f"inserted {len(rows_to_insert)}, done")
    else:
        print("no rows to insert")

def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]

async def process(dataset_id, table_id, flag):
    client = bigquery.Client()
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref) 
    rows_to_insert = []

    proc = await asyncio.create_subprocess_exec(ZDNS, "A", "-retries", "6",  "-iterative",
                                                stdout=PIPE, stdin=PIPE, limit=2**20)
                    
    await asyncio.gather(
        batch_writer(proc.stdin),
        batch_reader(proc.stdout, rows_to_insert))
    
    print("len of rows to insert: " , len(rows_to_insert))
    
    for row in batch(rows_to_insert, 1000):
        print("len of x: " , len(x))
        try:
            errors = client.insert_rows(table, x)
            print("errors: ", errors)
            flag.append(True)
        except Exception as e:
            print("Insert row exception: ", e)
            flag.append(False)
    

def update_big_table(dataset_id, table_id, bq_domain2ip_table):
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig()
    
    sql = ' UPDATE `{}.{}` A \
    SET A.site_ip = IFNULL( \
    (SELECT B.ip FROM `{}.{}` B \
    WHERE A.site_domain = B.domain \
    ), \
    A.site_ip \
    ), \
    A.load_ip = IFNULL( \
    (SELECT B.ip FROM `{}.{}` B \
    WHERE A.load_domain = B.domain \
    ), \
    A.load_ip \
    ) \
    where TRUE '.format(dataset_id,table_id,
                        dataset_id,bq_domain2ip_table,
                        dataset_id,bq_domain2ip_table)

    query_job = client.query(
        sql,
        job_config = job_config
    ) 

    print("Starting job {}".format(query_job.job_id))
    query_job.result()

def google_bigquery_storage_api(project_id, dataset_id, table_id):
    client = bigquery_storage_v1beta1.BigQueryStorageClient()
    table_ref = bigquery_storage_v1beta1.types.TableReference()
    table_ref.project_id = project_id
    table_ref.dataset_id = dataset_id
    table_ref.table_id = table_id

    parent = "projects/{}".format(project_id)
    session = client.create_read_session(
        table_ref,
        parent,
        format_=bigquery_storage_v1beta1.enums.DataFormat.AVRO,
        sharding_strategy=(bigquery_storage_v1beta1.enums.ShardingStrategy.LIQUID),
    )  

    domain = set()
    try:
        read_position = bigquery_storage_v1beta1.types.StreamPosition(stream=session.streams[0])
        reader = client.read_rows(read_position)
        rows = reader.rows(session)
        for row in rows:
            domain.add(row["domain"])
    except Exception as e:
        print(e)
    
    with open("temp.csv", "w") as f:
        for item in domain:
            f.write("%s\n" % item)
    print("Got {} unique domains".format(len(domain)))

def get_domain_list(project_id, dataset_id, bq_table_to_be_updated, bq_domain2ip_table, bq_domain_list):
    client = bigquery.Client()
    table_ref = client.dataset(dataset_id).table(bq_domain_list)
    job_config = bigquery.QueryJobConfig()
    job_config.destination = table_ref
    job_config.allow_large_results = True
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE #overwrites

    query_str = "select domain from (select distinct(site_domain) as domain \
                from `{}.{}` union distinct select distinct(load_domain) as domain \
                from `{}.{}`)sub where domain not in \
                (select domain from `{}.{}`)".format(
                dataset_id, bq_table_to_be_updated,
                dataset_id, bq_table_to_be_updated, 
                dataset_id, bq_domain2ip_table)
    
    print("executing: ", query_str)
    query = (
        query_str
    )
    
    query_job = client.query (
        query,
        location="US",
        job_config=job_config
    )

    query_job.result()
    print('Query results loaded to table {}'.format(table_ref.path))

    google_bigquery_storage_api(project_id, dataset_id, bq_domain_list)

def run_aggregation_query(dataset_id, bq_domain2ip_table):
    # aggregate ips corresponding to a domain 
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig()
    table_ref = client.dataset(dataset_id).table(bq_domain2ip_table)
    job_config.destination = table_ref
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE #overwrites

    query_str = "   SELECT domain, \
                STRING_AGG(ip ORDER BY ip) AS ip \
                FROM ipprivacy.subsetting.`{}` \
                GROUP BY domain ".format(bq_domain2ip_table)
    
    query = (
        query_str
    )
    
    query_job = client.query (
        query,
        location="US",
        job_config=job_config
    )

    query_job.result()
    print('Run_agg: Query results loaded to table {}'.format(table_ref.path))

def create_bq_table(dataset_id, table_id):
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig()
    table_ref = client.dataset(dataset_id).table(table_id)
    job_config.destination = table_ref
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE #overwrites

    query_str = ' SELECT P.pageid as pageid, R.requestid as requestid, \
                P.url as site_url, R.url as load_url, NET.REG_DOMAIN(P.url) as site_domain, \
                NET.REG_DOMAIN(R.url) as load_domain, P.cdn as site_cdn, \
                R._cdn_provider as load_cdn, R.type as type, R.mimeType as mimeType, "" as site_ip , "" as load_ip \
                FROM httparchive.summary_pages.`{}` as P \
                INNER JOIN httparchive.summary_requests.`{}` R ON P.pageid = R.pageid'.format(table_id, table_id)
    query = (
        query_str
    )
    
    query_job = client.query (
        query,
        location="US",
        job_config=job_config
    )

    query_job.result()
    print('Query results loaded to table {}'.format(table_ref.path))

if __name__ == "__main__":

    if len(sys.argv) < 2:
        print("Enter bq_table_to_be_updated (example: 2019_10_01_desktop) ")
        exit()

    project_id = "ipprivacy"
    dataset_id = "subsetting"
    bq_table_to_be_updated = sys.argv[1] 
    bq_domain2ip_table = "domain2ip"
    bq_domain_list = "domain_list"

    create_bq_table(dataset_id, bq_table_to_be_updated)
    get_domain_list(project_id, dataset_id, bq_table_to_be_updated, bq_domain2ip_table, bq_domain_list)
    flag = []
    asyncio.run(process(dataset_id, bq_domain2ip_table, flag))
    print(flag)
    if flag[-1]:
        run_aggregation_query(dataset_id, bq_domain2ip_table)
    else:
        print(f"flag: {flag[-1]} --> No aggregation needed")
    update_big_table(dataset_id, bq_table_to_be_updated, bq_domain2ip_table)