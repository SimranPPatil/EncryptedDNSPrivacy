import sys

if len(sys.argv) != 2:
    print("Enter in the format: python3 parse_shards.py filename")
    exit()

ipset_to_domain = dict()
with open(sys.argv[1]) as f:
    for line in f:
        ips = line.split("{")[1].split("}")[0].split(",")
        domain = line.split(",")[0]
        ipset = set()
        for ip in ips:
            ipset.add(ip)
        ipset = frozenset(ipset)
        ipset_to_domain.setdefault(ipset, set()).add(domain)

count = 0
for ipset in ipset_to_domain:
    if len(ipset_to_domain[ipset]) > 1:
        print(ipset, ipset_to_domain[ipset])
        count += 1

print("instances: ", count)