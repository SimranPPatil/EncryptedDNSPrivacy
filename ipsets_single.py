import json
import dns.resolver
import os
from urllib.parse import urlparse

sites_to_domains = dict()
domains_to_ip = dict()
ip = dict()
domains = set()
os.system('export PATH=$PATH:~/go/bin')

with open("1.json") as f:
    i = 0
    for line in f:
        i += 1
        obj = json.loads(line)
        url = urlparse(obj["requestURL"])
        domain = url.netloc
        sites_to_domains.setdefault(obj['siteURL'],set()).add(domain)
        cmd =  'echo ' + domain + ' | zdns A -retries 10'
        output = os.popen(cmd).readlines()
        for op in output:
            obj = json.loads(op)
            try:
                domain = obj['name']
                if obj['status'] == 'NOERROR':
                    answers = obj['data']['answers']
                    for answer in answers:
                        domains_to_ip.setdefault(domain, set()).add(answer['answer'])
                        ip.setdefault(answer['answer'], 0)
                        ip[answer['answer']] += 1
                else:
                    domains_to_ip.setdefault(domain, set()).add(obj['status'])
            except Exception as e:
                print("Exception: ", e)
    print(i)

for key in sites_to_domains:
    domain_list = ", ".join(sites_to_domains[key])
    sites_to_domains[key] = domain_list

for key in domains_to_ip:
    ip_list = ", ".join(domains_to_ip[key])
    domains_to_ip[key] = ip_list

with open("sites_to_domains_single.txt", "w") as f:
    json.dump(sites_to_domains, f)

with open("domains_to_ip_single.txt", "w") as f:
    json.dump(domains_to_ip, f)

with open("ip_freq_single.txt", "w") as f:
    json.dump(ip, f)