import json
import os
from urllib.parse import urlparse

sites_to_domains = dict()
domains_to_ip = dict()
ip = dict()
domains = set()
with open("1.json") as f:
    for line in f:
        obj = json.loads(line)
        url = urlparse(obj["requestURL"])
        domain = url.netloc
        sites_to_domains.setdefault(obj['siteURL'],set()).add(domain)
        if domain not in domains:
            domains.add(domain)
        
domain_list = '\n'.join(domains)
with open("domains.txt", "w") as f:
    f.write(domain_list)


# cat these
def performQueries(domain_list, domains_to_ip, ip):
    os.system('export PATH=$PATH:~/go/bin')
    cmd =  'cat domains.txt | zdns A -retries 10'
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

'''
temp = set()
num = 0
while len(domains) > 0:
    num += 1
    temp.add(domains.pop())
    if num == 500:
        num = 0
        domain_list = '\n'.join(temp)
        performQueries(domain_list, domains_to_ip, ip)
        temp.clear()
        
print("here")
'''

for key in sites_to_domains:
    domain_list = ", ".join(sites_to_domains[key])
    sites_to_domains[key] = domain_list

with open("sites_to_domains.txt", "w") as f:
    json.dump(sites_to_domains, f)

with open("domains_to_ip.txt", "w") as f:
    json.dump(domains_to_ip, f)

with open("ip_freq.txt", "w") as f:
    json.dump(ip, f)
