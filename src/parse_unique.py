# site, domain, ip, resources, cdn
import glob, sys, json

site_to_ip = dict()
ipset_to_sites = dict()
for filename in glob.glob("../output_old2/unique*"):
    with open(filename) as f:
        for line in f:
            try:
                row = line.split(",")
                site = row[0]
                ip = row[2]
                site_to_ip.setdefault(site, set()).add(ip)
            except Exception as e:
                print("Exception: ", e)
                exc_type, _, exc_tb = sys.exc_info()
                print(exc_type, exc_tb.tb_lineno, "\n\n")

for site in site_to_ip:
    ipset = site_to_ip[site]
    ipset = frozenset(ipset)
    ipset_to_sites.setdefault(str(ipset), set()).add(site)

total = 0
multiple = 0
for ipset in ipset_to_sites:
    total += 1
    if len(ipset_to_sites[ipset]) > 1:
        multiple += 1
    sites = ",".join(ipset_to_sites[ipset])
    ipset_to_sites[ipset] = sites

print("Total: ", total)
print("Multiple: ", multiple)
with open("unique_parsed.json", "w+") as f:
    json.dump(ipset_to_sites, f)
