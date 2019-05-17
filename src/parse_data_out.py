import glob, sys, re, json

#{set of sites}, {set of url}, domain, {set of ips}, {set of cdns}, {set of resources}
sites_to_ipsets = dict()
ipsets_to_sites = dict()
for filename in glob.glob("../output/data_out_*"):
    print(filename)
    if "2019_05_16" in filename:
        with open(filename) as f:
            print(filename)
            for line in f:
                try:
                    sites = line.split("}")[0].split("{")[1].split(",")
                    ips = line.split("}")[2].split("{")[1].split(",")
                    for site in sites:
                        for ip in ips:
                            sites_to_ipsets.setdefault(site, set()).add(ip)
                except Exception as e:
                    print("Exception: ", e, len(line.split("}")))
                    exc_type, _, exc_tb = sys.exc_info()
                    print(exc_type, exc_tb.tb_lineno, "\n\n")

for key in sites_to_ipsets:
    ipsets = sites_to_ipsets[key]
    ipsets = frozenset(ipsets)
    ipsets_to_sites.setdefault(str(ipsets), set()).add(key)
    ips = ",".join(sites_to_ipsets[key])
    sites_to_ipsets[key] = ips

total = 0
mul = 0
ipsets_to_sites_multiple = dict()
for key in ipsets_to_sites:
    total += 1
    if len(ipsets_to_sites[key]) > 1:
        sm = ",".join(ipsets_to_sites[key])
        ipsets_to_sites_multiple.setdefault(key, "")
        ipsets_to_sites_multiple[key] = sm
        mul += 1
    sites = ",".join(ipsets_to_sites[key])
    ipsets_to_sites[key] = sites

print("total: ", total)
print("multiple: ", mul)
with open("ips_to_sites.json", "w+") as f:
    try:
        json.dump(ipsets_to_sites, f)
    except Exception as e:
        print("Exception: ", e)
        exc_type, _, exc_tb = sys.exc_info()
        print(exc_type, exc_tb.tb_lineno, "\n\n")

with open("ip_to_sites_multiple.json", "w+") as fp:
    try:
        json.dump(ipsets_to_sites_multiple, fp)
    except Exception as e:
        print("Exception: ", e)
        exc_type, _, exc_tb = sys.exc_info()
        print(exc_type, exc_tb.tb_lineno, "\n\n")

with open("site_ips.json", "w+") as f:
    try:
        json.dump(sites_to_ipsets, f)
    except Exception as e:
        print("Exception: ", e)
        exc_type, _, exc_tb = sys.exc_info()
        print(exc_type, exc_tb.tb_lineno, "\n\n")
