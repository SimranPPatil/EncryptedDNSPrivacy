# mpdomains --> domain, {ips}, {cdns}, {resources}
# domain = line.split("{")[0].split(",")[0]
# ips = line.split("{")[1].split("}")[0].split(",")
# cdns = line.split("{")[2].split("}")[0].split(",")

# data_out --> {sites}, {urls}, domain, {ips}, {cdns}, {resources}

import sys, json

cdnssets_to_sites = dict()

with open(sys.argv[1]) as f:
    for line in f:
        if "CDN Missing" in line:
            continue
        else:
            try:
                cdnset = set()
                sites = line.split("}")[0].split("{")[1].split(",")
                cdns = line.split("}")[-2].split("{")[1].split(",")
                print(sites, cdns)
                for cdn in cdns:
                    cdnset.add(cdn)
                cdnset = frozenset(cdnset)
                for site in sites:
                    cdnssets_to_sites.setdefault(str(cdnset), set()).add(site)
            except Exception as e:
                print("Exception: ", e)
                exc_type, _, exc_tb = sys.exc_info()
                print(exc_type, exc_tb.tb_lineno, "\n\n")

for cdnset in cdnssets_to_sites:
    sites = ",".join(cdnssets_to_sites[cdnset])
    cdnssets_to_sites[cdnset] = sites

with open("cdnsets_to_sites.json", "w+") as f:
    json.dump(cdnssets_to_sites, f)