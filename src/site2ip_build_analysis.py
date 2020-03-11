import sys, json, os
import pyasn
from google.cloud import storage
from google.cloud import bigquery
from google.cloud import bigquery_storage_v1beta1
from difflib import SequenceMatcher

if len(sys.argv) < 3:
    print("enter table to be analyzed sql_analysis")
    exit()

project_id = 'ipprivacy'
dataset_id = 'subsetting'
site2ip = 'site2ip'
table_id = sys.argv[1]
site_ip_anonsets = 'site_ip_anonsets'
rows_to_insert = []
client = bigquery.Client()

def write_query_res_local(query_str, idx, query_job):
    with open("./"+table_id+"/"+str(idx)+'.txt', "w+") as f:
        print(query_str, file = f)
        print('\n\n', file = f)
        for row in query_job:
            print(row, file = f)
            print('\n', file = f)

def executeQuery(query_str, idx, write_bool):
    job_config = bigquery.QueryJobConfig()
    query = (
        query_str
    )
    query_job = client.query (
        query,
        location="US",
        job_config=job_config
    )
    query_job.result()

    if write_bool:
        write_query_res_local(query_str, idx, query_job)

    return query_job
    

def getQuery():
    with open(sys.argv[2], "r") as f:
        i = 0
        for line in f:
            i+=1
            query_str = line.format(
                table_analyzed=table_id,
                site_ip_anonsets=site_ip_anonsets)
            print(query_str + '\n')
            executeQuery(query_str, i, 1)

def createTable(table_name, sql):
    job_config = bigquery.QueryJobConfig()
    table_ref = client.dataset(dataset_id).table(table_name)
    job_config.destination = table_ref
    job_config.allow_large_results = True
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE #overwrites

    query_job = client.query(
        sql,
        location='US',
        job_config=job_config)

    query_job.result()
    print('Query results loaded to table {}'.format(table_ref.path))


def create_ip_to_asn(rows_to_insert):
    asndb = pyasn.pyasn('pyasn')
    ip_to_asn = dict()
    
    query_str = "select ip from (select distinct(ip) as ip from ipprivacy.subsetting.site2ip)sub \
        where ip not in (select distinct(ip) as ip from ipprivacy.subsetting.ip2asn)"
    query_job = executeQuery(query_str, 0, 0)

    for row in query_job:
        ip = "nil"
        try:
            ip = row['ip']
            asn,_= asndb.lookup(ip)
            ip_to_asn[ip] = asn
        except:
            print(ip, " not mapped to asn")
            continue
    
    print("ipasn: ", len(ip_to_asn), ip_to_asn)
    return
    for ip in ip_to_asn:
        rows_to_insert.append((ip, ip_to_asn[ip]))


def update_site2ip(site2ip, table_id):
    job_config = bigquery.QueryJobConfig()
    table_ref = client.dataset(dataset_id).table(site2ip)
    job_config.destination = table_ref
    job_config.allow_large_results = True
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND #appends

    query_str = "select site_domain as site, B.ip as ip from (select site_domain from (select distinct(site_domain) as \
                site_domain from ipprivacy.subsetting.`{}`) sub where site_domain \
                not in (select distinct(site) as site_domain from ipprivacy.subsetting.site2ip)) \
                inner join ipprivacy.subsetting.domain2ip B on site_domain = B.domain".format(table_id)

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

def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]

if __name__ == "__main__":
    # define the name of the directory to be created
    path = "./{}".format(table_id)

    try:
        os.mkdir(path)
    except OSError:
        print ("Creation of the directory %s failed" % path)
    else:
        print ("Successfully created the directory %s " % path)
    
    print("Current table: ", table_id)
    # update_site2ip(site2ip, table_id)

    site_ip_anonsets_sql = "select ip, count(distinct(site)) as cnt from `{}.{}.{}` group by ip;".format(project_id,dataset_id,site2ip)
    # createTable(site_ip_anonsets, site_ip_anonsets_sql)
    # getQuery()
    create_ip_to_asn(rows_to_insert)

    # add to ip2asn
    table_ref = client.dataset(dataset_id).table('ip2asn')
    table = client.get_table(table_ref) 

    for row in batch(rows_to_insert, 100):
        try:
            errors = client.insert_rows(table, row)
            print("rti errors: ", errors)
        except Exception as e:
            print("Insert row exception: ", e)