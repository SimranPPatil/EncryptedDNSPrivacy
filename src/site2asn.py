import sys, json, os
import pyasn
from google.cloud import storage
from google.cloud import bigquery
from google.cloud import bigquery_storage_v1beta1
from difflib import SequenceMatcher

if len(sys.argv) < 2:
    print("enter sql_analysis")
    exit()

project_id = 'ipprivacy'
dataset_id = 'subsetting'
site2asn = 'site2asn'
site_asn_anonsets = 'site_asn_anonsets'
client = bigquery.Client()

def create_site2asn(site2asn):
    job_config = bigquery.QueryJobConfig()
    table_ref = client.dataset(dataset_id).table(site2asn)
    job_config.destination = table_ref
    job_config.allow_large_results = True
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE #overwrites

    query_str = "select A.site as site, B.asn as asn \
        from ipprivacy.subsetting.site2ip A join ipprivacy.subsetting.ip2asn B on A.ip = B.ip"

    query = (
        query_str
    )
    
    query_job = client.query (
        query,
        location = "US",
        job_config=job_config
    )

    query_job.result()
    print('Query results loaded to table {}'.format(table_ref.path))

def create_asn_anonsets(site_asn_anonsets):
    job_config = bigquery.QueryJobConfig()
    table_ref = client.dataset(dataset_id).table(site_asn_anonsets)
    job_config.destination = table_ref
    job_config.allow_large_results = True
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE #overwrites

    query_str = "select asn, count(distinct(site)) as cnt from ipprivacy.subsetting.site2asn group by asn"

    query = (
        query_str
    )
    
    query_job = client.query (
        query,
        location = "US",
        job_config=job_config
    )

    query_job.result()
    print('Query results loaded to table {}'.format(table_ref.path))

def write_query_res_local(query_str, idx, query_job):
    with open("site2asn_" + str(idx)+'.txt', "w+") as f:
        print(query_str, file = f)
        print('\n\n', file = f)
        for row in query_job:
            print(row, file = f)
            print('\n', file = f)

def executeQuery(query_str, idx, write_bool):
    job_config = bigquery.QueryJobConfig()
    query_job = client.query(
        query_str,
        location="US",
        job_config=job_config
    )
    query_job.result()

    if write_bool:
        write_query_res_local(query_str, idx, query_job)

    return query_job

def getQuery():
    with open(sys.argv[1], "r") as f:
        i = 0
        for line in f:
            i+=1
            query_str = line.format(site_asn_anonsets = site_asn_anonsets) 
            print(query_str + '\n')
            executeQuery(query_str, i, 1)

if __name__ == "__main__":
    create_site2asn(site2asn)
    create_asn_anonsets(site_asn_anonsets)
    getQuery()