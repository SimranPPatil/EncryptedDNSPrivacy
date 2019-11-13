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

async def batch_reader(file, client, table):
    rows_to_insert = []
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
        if counter % BATCH_SIZE == 0:
            if len(rows_to_insert):
                errors = client.insert_rows(table, rows_to_insert)
                print(f"inserted {counter}, errors: ", len(errors))
                rows_to_insert = []
    if len(rows_to_insert):
        errors = client.insert_rows(table, rows_to_insert)
        print(f"inserted {counter}, done, errors: ", len(errors))
    else:
        print("no rows to insert")

async def process(dataset_id, table_id):
    client = bigquery.Client()
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref) 

    proc = await asyncio.create_subprocess_exec(ZDNS, "A", "-retries", "6",  "-iterative",
                                                stdout=PIPE, stdin=PIPE, limit=2**20)
    await asyncio.gather(
        batch_writer(proc.stdin),
        batch_reader(proc.stdout, client, table))

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

def google_bigquery_storage_api(project_id, dataset_id, table_id):
    client = bigquery_storage_v1beta1.BigQueryStorageClient()
    # This example reads baby name data from the public datasets.
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
    )  # API request.

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

    query_str = "   select domain from (select distinct(site_domain) as domain \
                from `{}.{}` union distinct select distinct(load_domain) as domain \
                from `{}.{}`)sub where domain not in \
                (select domain from `{}.{}`) ".format(dataset_id, bq_table_to_be_updated,
                dataset_id, bq_table_to_be_updated, 
                dataset_id, bq_domain2ip_table)
    
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
    print('Query results loaded to table {}'.format(table_ref.path))

if __name__ == "__main__":

    if len(sys.argv) < 6:
        print("Enter project_id dataset_id bq_table_to_be_updated bq_domain2ip_table bq_domain_list")
        exit()

    project_id = sys.argv[1] #"ipprivacy"
    dataset_id = sys.argv[2] #"subsetting"
    bq_table_to_be_updated = sys.argv[3] #"summary_requests_domain"
    bq_domain2ip_table = sys.argv[4] #domain_list_curr_from_gcs
    bq_domain_list = sys.argv[5] #domain2ip.db

    get_domain_list(project_id, dataset_id, bq_table_to_be_updated, bq_domain2ip_table, bq_domain_list)
    asyncio.run(process(dataset_id, bq_domain2ip_table))
    run_aggregation_query(dataset_id, bq_domain2ip_table)
    update_big_table(dataset_id, bq_table_to_be_updated, bq_domain2ip_table)