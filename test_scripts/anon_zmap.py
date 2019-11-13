import sys
import asyncio
from asyncio.subprocess import PIPE
import json
import aiosqlite
from ipaddress import IPv4Address, AddressValueError

BATCH_SIZE = 1000

MODE = "domains"
table, column = "crawl_data", "load_domain"
if len(sys.argv) > 2:
    MODE = sys.argv[2]

SEEN_QUERY = "SELECT DISTINCT(domain) from domain2ip"
if MODE == "sites":
    table, column = "sites", "domain"
elif MODE == "rdns":
    table, column = "domain2ip", "ip"
    SEEN_QUERY = "SELECT ip FROM rdns"

QUERY = f"""
select DISTINCT({column}) from {table}
WHERE {column} IS NOT NULL
"""


async def batch_writer(db, file):
    seen = set()
    threshold = 1
    print("Checking already looked up domains")
    async with db.execute(SEEN_QUERY) as cursor:
        async for row in cursor:
            seen.add(row[0])
    print(f"{len(seen)} domains already done, starting query")
    done_cnt = len(seen)
    async with db.execute(QUERY) as cursor:
        print("Iterating")
        async for row in cursor:
            if row[0] not in seen:
                if MODE == "rdns":
                    data = str(IPv4Address(int(row[0])))
                else:
                    data = row[0]
                file.write(f"{data}\n".encode())
                await file.drain()
                seen.add(row[0])
                new_cnt = len(seen) - done_cnt
                if new_cnt > threshold:
                    print(f"SQL: {new_cnt}, {data}")
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
        if MODE == "rdns":
            if 'data' in data and 'answers' in data['data'] and data['data']['answers']:
                for a in data['data']['answers']:
                    print(domain, a)
                    if a['type'] == "PTR":
                        await db.execute("insert or ignore into rdns values (?,?)", (int(IPv4Address(domain)),
                                         a['answer'].strip('.')))
                counter += 1
                # FIXME: DRY
                if counter % BATCH_SIZE == 0:
                    print("commit...")
                    await db.commit()
                    print(f"committed {counter}")
        else:
            if 'data' in data and 'ipv4_addresses' in data['data']:
                for a in data['data']['ipv4_addresses']:
                    try:
                        ipv4 = IPv4Address(a)
                        await db.execute('insert or ignore into domain2ip values (?,?)',
                                         (domain, int(ipv4)))
                    except AddressValueError:
                        print(f"AddressValueError: {domain}, {a}")
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
        if MODE == "rdns":
            zmode = "PTR"
        else:
            zmode = "ALOOKUP"
        proc = await asyncio.create_subprocess_exec(ZDNS, zmode, "-retries", "6",  "-iterative",
                                                    stdout=PIPE, stdin=PIPE, limit=2**20)
        await asyncio.gather(batch_writer(db, proc.stdin),
                             batch_reader(db, proc.stdout))


asyncio.run(process())