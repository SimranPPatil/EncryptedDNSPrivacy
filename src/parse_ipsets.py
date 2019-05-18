# mpdomains --> domain, {ips}, {cdns}, {resources}
# domain = line.split("{")[0].split(",")[0]
# ips = line.split("{")[1].split("}")[0].split(",")
# cdns = line.split("{")[2].split("}")[0].split(",")

# data_out --> {sites}, {urls}, domain, {ips}, {cdns}, {resources}
# sites = line.split("}")[0].split("{")[1].split(",")

import sys, json

sites_to_ips = dict
ipsets_to_domains = dict()

with open(sys.argv[1]) as f:
    for line in f:
        try:
            ipset = set()
            ips = line.split("}")[2].split("{")[1].split(",")
            domain = line.split("}")[2].split("{")[0].split(",")[1]
            for ip in ips:
                ipset.add(ip)
            ipset = frozenset(ipset)
            ipsets_to_domains.setdefault(str(ipset), set()).add(domain)
        except Exception as e:
            print("Exception: ", e)
            exc_type, _, exc_tb = sys.exc_info()
            print(exc_type, exc_tb.tb_lineno, "\n\n")

for ipset in ipsets_to_domains:
    domains = ",".join(ipsets_to_domains[ipset])
    ipsets_to_domains[ipset] = domains

with open("ipsets_to_domains.json", "w+") as f:
    json.dump(ipsets_to_domains, f)