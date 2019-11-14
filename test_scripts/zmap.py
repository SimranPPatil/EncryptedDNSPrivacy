import sys
import json
import asyncio
from asyncio.subprocess import PIPE
import aiosqlite
import aiofiles
from os.path import expanduser

if len(sys.argv) < 4:
        print("Enter Database to be populated, json_file, table_type")
        exit()

ZDNS = expanduser("~/go/bin/zdns")
BATCH_SIZE = 1000
SEEN_SITE = "SELECT DISTINCT(site_domain) from site_domain2ip"
SEEN_LOAD = "SELECT DISTINCT(load_domain) from load_domain2ip"
TYPE = None
INSERT = None

if sys.argv[3] == "site":
    SEEN_QUERY = SEEN_SITE
    TYPE = 'site_domain'
    INSERT = "insert or ignore into site_domain2ip values (?,?)"
elif sys.argv[3] == "load":
    SEEN_QUERY = SEEN_LOAD
    TYPE = 'load_domain'
    INSERT = "insert or ignore into load_domain2ip values (?,?)"
else:
    SEEN_QUERY = "SELECT DISTINCT(domain) from domain2ip"
    TYPE = "domain"
    INSERT = "insert or ignore into domain2ip values (?,?)"

FILENAME = sys.argv[2]
fp = open("no_ip.txt", "w")

async def batch_writer(db, file_in):
    async with aiofiles.open(FILENAME, "r") as json_file:
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
                    print("skipped: ", data)
                counter += 1
                if counter % BATCH_SIZE == 0:
                    print("commit...")
                    await db.commit()
                    print(f"committed {counter}")
        else:
            try:
                await db.execute(INSERT, (domain, "NONE"))
            except:
                print("skipped: ", data)
            counter += 1
            if counter % BATCH_SIZE == 0:
                print("commit...")
                await db.commit()
                print(f"committed {counter}")
    await db.commit()
    print(f"committed {counter}, done")

async def process():
    async with aiosqlite.connect(sys.argv[1]) as db:
        print("Connected")
        proc = await asyncio.create_subprocess_exec(ZDNS, "A", "-retries", "6",  "-iterative",
                                                    stdout=PIPE, stdin=PIPE, limit=2**20)
        await asyncio.gather(
            batch_writer(db, proc.stdin),
            batch_reader(db, proc.stdout))

asyncio.run(process())