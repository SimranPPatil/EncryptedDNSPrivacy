from google.cloud import storage
import os
import sqlite3

def dbTojson(filename):
    conn = sqlite3.connect('domains.db')
    cur = conn.cursor()
    query = 'SELECT * from ' + filename
    result = cur.execute(query)

    ld = [dict(zip([key[0] for key in cur.description], row)) for row in result]
    with open(filename+'.json', 'w+') as outfile:
        for l in ld:
            json.dump(l, outfile)
            outfile.write("\n")

def download_blob(bucket_name, source_blob_name, destination_file_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    blob.download_to_filename(destination_file_name)

    print('Blob {} downloaded to {}.'.format(
        source_blob_name,
        destination_file_name))

if __name__ == "__main__":
    
    bucket_name = 'domains_2019_07_01'
    for i in range(1,88):
        source_blob_name = 'domains-0000000000{:02d}.json'.format(i)
        download_blob(bucket_name, source_blob_name, source_blob_name)
        print("calling:  python3 zmap.py domains.db " + source_blob_name + " site")
        os.system("python3 zmap.py domains.db " + source_blob_name + " site")
        print("calling:  python3 zmap.py domains.db " + source_blob_name + " load")
        os.system("python3 zmap.py domains.db " + source_blob_name + " load")
        os.remove(source_blob_name)
    
    dbTojson('site_domain2ip')
    dbTojson('load_domain2ip')