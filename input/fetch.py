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

ZDNS = expanduser("~/go/bin/zdns")
BATCH_SIZE = 100
SEEN_QUERY = "SELECT DISTINCT(domain) from domain2ip"
TYPE = "domain"
INSERT = "insert or ignore into domain2ip values (?,?)"

async def batch_writer(db, file_in, filename):
    async with aiofiles.open(filename, "r") as json_file:
        seen = set()
        threshold = 1
        print("Checking already looked up domains")
        async with db.execute(SEEN_QUERY) as cursor:
            async for row in cursor:
                print("row is: ", row, row[0])
                seen.add(row[0])
        print(f"{len(seen)} domains already done, starting query")
        done_cnt = len(seen)
        async for entry in json_file:
            entry = json.loads(entry)
            try:
                domain = entry[TYPE]
            except:
                domain = None
            print(TYPE, domain)

            if domain not in seen:
                file_in.write(f"{domain}\n".encode())
                await file_in.drain()
                seen.add(domain)
                new_cnt = len(seen) - done_cnt
                if new_cnt > threshold:
                    print(f"SQL: {new_cnt}, {domain}")
                    threshold *= 1.25
        print(f"Sent {new_cnt} items")
    file_in.write_eof()
    await file_in.drain()

async def batch_reader(db, file):
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
                    await db.execute(INSERT, (domain, a['answer'].strip('.')))
                except:
                    await db.execute(INSERT, (domain, "NONE"))
        else:
            await db.execute(INSERT, (domain, "NONE"))
        counter += 1
        if counter % BATCH_SIZE == 0:
            print("commit...")
            await db.commit()
            print(f"committed {counter}")
    await db.commit()
    print(f"committed {counter}, done")

async def process(filename):
    async with aiosqlite.connect(sys.argv[1]) as db:
        print("Connected")
        proc = await asyncio.create_subprocess_exec(ZDNS, "A", "-retries", "6",  "-iterative",
                                                    stdout=PIPE, stdin=PIPE, limit=2**20)
        await asyncio.gather(
            batch_writer(db, proc.stdin, filename),
            batch_reader(db, proc.stdout))

def execute_query(dataset_id, table_id, query_str):
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig()
    table_ref = client.dataset(dataset_id).table(table_id)
    job_config.destination = table_ref
    job_config.allow_large_results = True
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE #overwrites
 
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

def load_table_from_gcs(dataset_id, uri, table_id):
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset_id)
    job_config = bigquery.LoadJobConfig()
    job_config.schema = [
        bigquery.SchemaField("domain", "STRING"),
        bigquery.SchemaField("ip", "STRING"),
    ]
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.write_disposition = 'WRITE_TRUNCATE'
    load_job = client.load_table_from_uri(
        uri,
        dataset_ref.table(table_id),
        location="US",  # Location must match that of the destination dataset.
        job_config=job_config,
    )  # API request
    print("Starting job {}".format(load_job.job_id))

    load_job.result()  # Waits for table load to complete.
    print("Job finished.")

    destination_table = client.get_table(dataset_ref.table(table_id))
    print("Loaded {} rows.".format(destination_table.num_rows))

def update_big_table(dataset_id, final_table, table_from_gcs):
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
    where TRUE '.format(dataset_id,final_table,
                        dataset_id,table_from_gcs,
                        dataset_id,table_from_gcs)

    query_job = client.query(
        sql,
        job_config = job_config
    ) 

    print("Starting job {}".format(query_job.job_id))

def get_domain_list_and_export(project, dataset_id, final_table, table_id, bucket_name):
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig()
    # job_config.use_legacy_sql = True
    table_ref = client.dataset(dataset_id).table(table_id)
    job_config.destination = table_ref
    job_config.allow_large_results = True
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE #overwrites

    query_str = ' select distinct(site_domain) as domain \
            from `{}.{}` union distinct \
            select distinct(load_domain) as domain \
            from `{}.{}`'.format(dataset_id, final_table, dataset_id, final_table)
    
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

    destination_uri = "gs://{}/{}".format(bucket_name, "domain_list-*.json")
    dataset_ref = client.dataset(dataset_id, project=project)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.ExtractJobConfig()
    job_config.destination_format = 'NEWLINE_DELIMITED_JSON'

    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        location="US",
        job_config = job_config
    )  
    extract_job.result()  # Waits for job to complete.

    print(
        "Exported {}:{}.{} to {}".format(project, dataset_id, table_id, destination_uri)
    )

    return destination_uri

def download_blobs(bucket_name, url_format):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = storage_client.list_blobs(bucket_name)
    valid_uri = []
    for blob in blobs:
        if url_format in blob.name:
            valid_uri.append(blob.name)
            blob.download_to_filename(blob.name)
    return valid_uri

def dbTojson(table_name):
    conn = sqlite3.connect(sys.argv[1])
    cur = conn.cursor()
    query = 'SELECT * from ' + table_name
    result = cur.execute(query)
    file_generated = table_name+'.json'
    ld = [dict(zip([key[0] for key in cur.description], row)) for row in result]
    with open(file_generated, 'w+') as outfile:
        for l in ld:
            json.dump(l, outfile)
            outfile.write("\n")
    
    return file_generated

def upload_json_to_gcs(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print('File {} uploaded to {}.'.format(
        source_file_name,
        destination_blob_name))

def run_aggregation_query(table_name, dataset_id, table_id):
    # aggregate ips corresponding to a domain 
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig()
    table_ref = client.dataset(dataset_id).table(table_id)
    job_config.destination = table_ref
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE #overwrites

    query_str = "   SELECT domain, \
                STRING_AGG(ip ORDER BY ip) AS ip \
                FROM ipprivacy.subsetting.`{}` \
                GROUP BY domain ".format(table_name)
    
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

    if len(sys.argv) < 3:
        print("Enter Database to be populated, gcs_json_table_name")
        exit()

    project = "ipprivacy"
    dataset_id = "subsetting"
    final_table = "summary_requests_domain"
    table_id = "domain_list_curr"
    bucket_name = 'domains_2019_07_01'
    url_format = 'domain_list-'

    destination_uri = get_domain_list_and_export(project, dataset_id, final_table, table_id, bucket_name)
    valid_uri = download_blobs(bucket_name, url_format)
    
    for uri in valid_uri:
        print("uri: ", uri)
        asyncio.run(process(uri))

    file_generated = dbTojson(sys.argv[2])
    upload_json_to_gcs(bucket_name,file_generated,file_generated)

    source_uri = "gs://"+bucket_name+"/"+file_generated
    load_table_from_gcs(dataset_id, source_uri, table_id+"_from_gcs")
    run_aggregation_query(table_id+"_from_gcs", dataset_id, "agg_"+table_id+"_from_gcs")
    update_big_table(dataset_id, final_table, "agg_"+table_id+"_from_gcs")
