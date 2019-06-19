import sys
import sqlite3
import asyncio
from asyncio.subprocess import PIPE
import json
import aiosqlite
from ipaddress import IPv4Address
from difflib import SequenceMatcher

BATCH_SIZE = 1000

table, column = "crawl_data", "load_domain"
if len(sys.argv) > 2:
    if sys.argv[2] == "sites":
        table, column = "sites", "domain"
    elif sys.argv[2] == "bq":
        table, column = "bq_crawl", "domain"
QUERY = f"""
select DISTINCT({column}) from {table}
WHERE {column} IS NOT NULL
"""

def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Exception as e:
        print(e)

cdn_map = []
with open('cnamechain.json') as f:
    cdn_map = json.load(f)

def get_cdn(answer, cdn_map):
    lengths = []
    for key in cdn_map:
        match = SequenceMatcher(None, key[0], answer).find_longest_match(0, len(key[0]), 0, len(answer))
        lengths.append(len(key[0][match.a: match.a + match.size]))
    maxLen = max(lengths)
    index = lengths.index(maxLen)
    return cdn_map[index][1]

async def batch_writer(db, file):
    seen = set()
    threshold = 1
    print("Checking already looked up domains")
    async with db.execute("SELECT DISTINCT(domain) FROM domain2ip") as cursor:
        async for row in cursor:
            seen.add(row[0])
    print(f"{len(seen)} domains already done, starting query")
    done_cnt = len(seen)
    async with db.execute(QUERY) as cursor:
        print("Iterating")
        async for row in cursor:
            if row[0] not in seen:
                file.write(f"{row[0]}\n".encode())
                await file.drain()
                seen.add(row[0])
                new_cnt = len(seen) - done_cnt
                if new_cnt > threshold:
                    print(f"SQL: {new_cnt}, {row[0]})")
                    threshold *= 1.25
        print(f"Sent {new_cnt} items")
    file.write_eof()
    await file.drain()

async def batch_reader(db, file):
    counter = 0
    while True:
        line = await file.readline()
        if not line:
            break

        data = json.loads(line)
        domain = data['name']
        if 'data' in data and 'answers' in data['data'] and \
                data['data']['answers']:
            changed = False
            for a in data['data']['answers']:
                if a['type'] == 'A':
                    print(domain, a['answer'], int(IPv4Address(a['answer'])))
                    await db.execute('insert or ignore into domain2ip values (?,?)',
                                     (domain, int(IPv4Address(a['answer']))))
                    changed = True
                if a['type'] == 'CNAME':
                    cdn = get_cdn(a['answer'], cdn_map)
                    print("CNAME", domain, a['answer'], cdn)
                    await db.execute('insert or ignore into cnames values (?,?)', (domain, a['answer']))
                    await db.execute('insert or ignore into cnames_cdn values (?,?,?)', (domain, a['answer'],cdn))
                    changed = True
            if changed:
                counter += 1
                if counter % BATCH_SIZE == 0:
                    print("commit...")
                    await db.commit()
                    print(f"committed {counter}")
    await db.commit()
    print(f"committed {counter}, done")

from os.path import expanduser
ZDNS = expanduser("~/go/bin/zdns")

async def process():
    async with aiosqlite.connect(sys.argv[1]) as db:
        print("Connected")
        # sql_create_cdn_table = """ CREATE TABLE IF NOT EXISTS cnames_cdn (
        #                                 domain TEXT,
        #                                 cname TEXT,
        #                                 cdn TEXT); """
        # create_table(db, sql_create_cdn_table)
        proc = await asyncio.create_subprocess_exec(ZDNS,
                                                    "A", "-retries", "6", "-iterative",
                                                    stdout=PIPE, stdin=PIPE)
        await asyncio.gather(batch_writer(db, proc.stdin),
                             batch_reader(db, proc.stdout))

asyncio.run(process())