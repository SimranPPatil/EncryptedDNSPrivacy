import json
import os, sys
from urllib.parse import urlparse
from datetime import datetime

today = str(datetime.now()).split(' ')[0]
print(today)

if len(sys.argv) != 2:
    print("Enter in the format: python3 ipsets.py <name of input json file>")
    exit()

input_json = sys.argv[1]
os.system("export PATH=$PATH:~/go/bin")

sites_to_domains = dict()
domains_to_ip = dict()
ip = dict()
domains = set()
with open(input_json) as f:
    for line in f:
        obj = json.loads(line)
        url = urlparse(obj["requestURL"])
        domain = url.netloc
        sites_to_domains.setdefault(obj['siteURL'],set()).add(domain)
        if domain not in domains:
            domains.add(domain)
        
domain_list = '\n'.join(domains)
with open("../output/domains"+today+".txt", "w") as f:
    f.write(domain_list)


# cat these
def performQueries(domain_list, domains_to_ip, ip):
    cmd =  'cat ../output/domains'+today+'.txt | zdns A -retries 10'
    output = os.popen(cmd).readlines()
    for op in output:
        obj = json.loads(op)
        try:
            domain = obj['name']
            if obj['status'] == 'NOERROR':
                answers = obj['data']['answers']
                for answer in answers:
                    domains_to_ip.setdefault(domain, []).append(answer['answer'])
                    ip.setdefault(answer['answer'], 0)
                    ip[answer['answer']] += 1
            else:
                domains_to_ip[domain] = obj['status']
        except Exception as e:
            print(e)

performQueries(domain_list, domains_to_ip, ip)

for key in sites_to_domains:
    domain_list = ", ".join(sites_to_domains[key])
    sites_to_domains[key] = domain_list

with open("../output/sites_to_domains"+today+".txt", "w") as f:
    json.dump(sites_to_domains, f)

with open("../output/domains_to_ip"+today+".txt", "w") as f:
    json.dump(domains_to_ip, f)

with open("../output/ip_freq"+today+".txt", "w") as f:
    json.dump(ip, f)
