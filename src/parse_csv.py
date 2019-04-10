import json
import os, sys
from urllib.parse import urlparse
from datetime import datetime
import csv

today = str(datetime.now()).split(' ')[0]
print("Run on: ", today)
os.system("export PATH=$PATH:~/go/bin")

if len(sys.argv) != 2:
    print("Enter in the format: python3 parse_csv.py <file>")
    exit()

# can get object type using a get request -- content header
# might have to create a tuple with the domain name unparsed and the object type
input_data = sys.argv[1] # data file
sites_to_domains = dict()
domains_to_ip = dict()
ip = dict()
domains = set()

domain_to_sites = dict()
ip_to_domains = dict()
ip_to_sites = dict()
domain_to_resources = dict()

with open(input_data) as f:
    for row in f:
        row = row.split(',')
        print(row)
        url = urlparse(row[3])
        domain = url.netloc
        sites_to_domains.setdefault(row[1],set()).add(domain)
        domain_to_sites.setdefault(domain, []).append(row[1])
        domain_to_resources.setdefault(domain, set()).add(row[4])
        if domain not in domains:
            domains.add(domain)

input_data = input_data.split(".csv")[0].split("/")[-1]
domain_list = '\n'.join(domains)
with open("../output/domains"+str(input_data)+"_"+today+".txt", "w") as f:
    f.write(domain_list)

def performQueries(domain_list, domains_to_ip, ip):
    cmd =  'cat ../output/domains'+str(input_data)+'_'+today+'.txt | zdns A -retries 10'
    output = os.popen(cmd).readlines()
    for op in output:
        obj = json.loads(op)
        try:
            domain = obj['name']
            if obj['status'] == 'NOERROR':
                answers = obj['data']['answers']
                for answer in answers:
                    ip_addr = answer['answer']
                    domains_to_ip.setdefault(domain, []).append(ip_addr)
                    ip_to_domains.setdefault(ip_addr, []).append(domain)
                    site = domain_to_sites[domain]
                    ip_to_sites.setdefault(ip_addr, []).append(site) 
                    ip.setdefault(ip_addr, 0)
                    ip[ip_addr] += 1
            else:
                domains_to_ip[domain] = obj['status']
        except Exception as e:
            print(e)

performQueries(domain_list, domains_to_ip, ip)

for key in sites_to_domains:
    domain_list = ", ".join(sites_to_domains[key])
    sites_to_domains[key] = domain_list

for key in domain_to_resources:
    resources = ", ".join(domain_to_resources[key])
    domain_to_resources[key] = resources

with open("../output/sites_to_domains"+str(input_data)+"_"+today+".txt", "w") as f:
    json.dump(sites_to_domains, f)

with open("../output/domains_to_ip"+str(input_data)+"_"+today+".txt", "w") as f:
    json.dump(domains_to_ip, f)

with open("../output/ip_freq"+str(input_data)+"_"+today+".txt", "w") as f:
    json.dump(ip, f)

# for ip unique to a site -- what resources are served?
domain_to_cdn = dict()
count_unique = 0
with_cdn = 0
for ip in ip_to_sites:
    if len(ip_to_sites[ip]) == 1:
        # here the ip is unique
        domains = ip_to_domains[ip]
        print("len of domains: ", len(domains))
        for domain in domains:
            count_unique += 1
            cmd = 'docker run -it --rm turbobytes/cdnfinder cdnfindercli --phantomjsbin="/bin/phantomjs" --host ' + str(domain)
            output = os.popen(cmd).readlines()
            if len(output[1].strip('\n')) != 0:
                with_cdn += 1
                domain_to_cdn.setdefault(domain, []).append(output[1].strip('\n'))

print("count_unique: ", count_unique)
print("cdn present: ", with_cdn)

with open("../output/domain_to_resources"+str(input_data)+"_"+today+".txt", "w") as f:
    json.dump(domain_to_resources, f)

with open("../output/domain_to_cdn"+str(input_data)+"_"+today+".txt", "w") as f:
    json.dump(domain_to_cdn, f)

with open("../output/ip_to_domains"+str(input_data)+"_"+today+".txt", "w") as f:
    json.dump(ip_to_domains, f)

with open("../output/ip_to_sites"+str(input_data)+"_"+today+".txt", "w") as f:
    json.dump(ip_to_sites, f)



