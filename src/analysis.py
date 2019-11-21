import sys
from google.cloud import storage
from google.cloud import bigquery
from google.cloud import bigquery_storage_v1beta1

client = bigquery.Client()
project_id = 'ipprivacy'
dataset_id = 'subsetting'
domain2ip = 'domain2ip'
table_analyzed = '2019_05_01_desktop'
site_domains = table_analyzed+'_site_domains'
ip_anonsets = table_analyzed+'_ip_anonsets'

def getQuery():
    with open(sys.argv[1], "r") as f:
        i = 0
        for line in f:
            i+=1
            query_str = line.format(
                table_analyzed=table_analyzed, 
                site_domains=site_domains,
                ip_anonsets=ip_anonsets,
                domain2ip=domain2ip)
            print(query_str + '\n')
            executeQuery(query_str, i)

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

def executeQuery(query_str, idx):
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
    with open(str(idx)+'.txt', "w+") as f:
        for row in query_job:
            print(row, file = f)
            print('\n', file = f)

if __name__ == "__main__":

    if len(sys.argv) < 2:
        print("enter sql file")
        exit()

    site_domains_sql = "select distinct load_domain, pageid from `{}.{}.{}` where load_domain is not null;".format(project_id,
    dataset_id, table_analyzed)
    #createTable(site_domains, site_domains_sql)

    ip_anonsets_sql = "select ip, count(distinct(domain)) as cnt from `{}.{}.{}` group by ip;".format(project_id,dataset_id,domain2ip)
    #createTable(ip_anonsets, ip_anonsets_sql)

    getQuery()