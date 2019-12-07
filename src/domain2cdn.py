import json
from google.cloud import bigquery
from difflib import SequenceMatcher

def load_cdn_map():
    cdn_map = []
    with open('../input/cnamechain.json') as f:
        cdn_map = json.load(f)
    return cdn_map

def get_cdn(answer, cdn_map):
        lengths = []
        for key in cdn_map:
            match = SequenceMatcher(None, key[0], answer).find_longest_match(0, len(key[0]), 0, len(answer))
            lengths.append(len(key[0][match.a: match.a + match.size]))
        maxLen = max(lengths)
        index = lengths.index(maxLen)
        return cdn_map[index][1]

def batch(iterable, n=1):
        l = len(iterable)
        for ndx in range(0, l, n):
            yield iterable[ndx:min(ndx + n, l)]

def get_cdn_mappings(rows_to_insert):
    client = bigquery.Client()
    query_str = "select domain from (select distinct(domain) as domain\
            from ipprivacy.subsetting.domain2ip) sub where domain not in\
            (select distinct(domain) from ipprivacy.subsetting.domain2cdn)"
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

    for row in query_job:
        try:
            domain = row['domain']
            cdn = get_cdn(domain, cdn_map)
            if len(cdn) > 0:
                print(domain, cdn)
                rows_to_insert.append((domain, cdn))
        except:
            continue

def populate_domain2cdn(rows_to_insert):
    project_id = "ipprivacy"
    dataset_id = "subsetting"
    client = bigquery.Client()
    table_ref = client.dataset(dataset_id).table("domain2cdn")
    table = client.get_table(table_ref) 

    for row in batch(rows_to_insert, 1000):
        try:
            errors = client.insert_rows(table, row)
            print("errors: ", errors)
        except Exception as e:
            print("Insert row exception: ", e)

if __name__ == "__main__":
    
    cdn_map = load_cdn_map()
    rows_to_insert = []
    get_cdn_mappings(rows_to_insert)
    populate_domain2cdn(rows_to_insert)

        