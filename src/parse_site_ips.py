import json, glob
ips_to_sites = dict()

for filename in glob.glob("./site_ips*"):
    with open(filename) as f:
        data = json.load(f)
        for key in data:
            ips = data[key].split(", ")
            ipset = set()
            for ip in ips:
                ipset.add(ip)
            ipset = frozenset(ipset)
            ips_to_sites.setdefault(str(ipset), set()).add(key)
total = 0
multiple = 0
for key in ips_to_sites:
    total += 1
    if len(ips_to_sites[key]) > 1:
        multiple += 1
    sites = ",".join(ips_to_sites[key])
    ips_to_sites[key] = sites

with open("ipsets.json", "w+") as f:
    json.dump(ips_to_sites, f)
        
print("Total: ", total)
print("Multiple: ", multiple)
            