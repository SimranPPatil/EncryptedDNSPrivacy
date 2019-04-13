import json
import os, sys
from urllib.parse import urlparse
from datetime import datetime
import csv
import shelve
from difflib import SequenceMatcher


today = str(datetime.now()).split(' ')[0]
print("Run on: ", today)

cdn_map = []
with open('cnamechain.json') as f:
    cdn_map = json.load(f)

if len(sys.argv) != 2:
    print("Enter in the format: python3 parse_csv.py <file> -- <file> should be in the format ../input/filename")
    exit()

input_data = sys.argv[1] 
in_name = input_data.split(".csv")[0].split("/")[-1]

domains_to_sites = shelve.open('domains_to_sites_'+str(in_name)+"_"+today)
ip_to_domains = shelve.open('ip_to_domains_'+str(in_name)+"_"+today)
ip_to_sites = shelve.open('ip_to_sites_'+str(in_name)+"_"+today)
domain_to_resources = shelve.open('domain_to_resources_'+str(in_name)+"_"+today)
domain_to_cdn = shelve.open('domain_to_cdn_'+str(in_name)+"_"+today)

ind = 0
with open("../output/domains_"+str(in_name)+"_"+today+".txt", "w") as final:
    with open(input_data) as f:
        for row in f:
            try:
                ind += 1
                if ind == 5000:
                    break
                row = row.split(',')
                url = urlparse(row[3])
                domain = url.netloc
                if (len(domain)) == 0:
                    continue
                try: 
                    db = domain_to_resources[domain]
                except KeyError:
                    line = domain + "\n"
                    final.write(line)
                resource = row[4].strip("\n")
                if len(resource) > 10:
                    continue
        
                domain_to_resources.setdefault(domain, set())
                domain_to_resources[domain] = domain_to_resources[domain].union(set([resource]))
                domains_to_sites.setdefault(domain, set())
                domains_to_sites[domain] = domains_to_sites[domain].union(set([row[1]]))
            except Exception as e:
                print("Exception: ", e)
                exc_type, _, exc_tb = sys.exc_info()
                print(row, exc_type, exc_tb.tb_lineno, "\n\n")
        
def get_cdn(answer, cdn_map):
    lengths = []
    for key in cdn_map:
        match = SequenceMatcher(None, key[0], answer).find_longest_match(0, len(key[0]), 0, len(answer))
        lengths.append(len(key[0][match.a: match.a + match.size]))
    maxLen = max(lengths)
    index = lengths.index(maxLen)
    return cdn_map[index][1]

def performQueries(domain_to_cdn, domains_to_sites, ip_to_domains, ip_to_sites, cdn_map):
    cmd =  'cat ../output/domains_'+str(in_name)+'_'+today+'.txt | ~/go/bin/zdns A -retries 10'
    output = os.popen(cmd).readlines()
    for op in output:
        obj = json.loads(op)
        try:
            domain = obj['name']
            site = domains_to_sites[domain]
            site = next(iter(site))
            if obj['status'] == 'NOERROR':
                answers = obj['data']['answers']
                for answer in answers:
                    if answer["type"] == "CNAME":
                        answer_ret = answer["answer"]
                        cdn = get_cdn(answer_ret, cdn_map)
                        domain_to_cdn.setdefault(domain, set())
                        domain_to_cdn[domain] = domain_to_cdn[domain].union(set([cdn]))
                    elif answer["type"] == "A":
                        ip = answer["answer"]
                        ip_to_domains.setdefault(ip, set())
                        ip_to_domains[ip] = ip_to_domains[ip].union(set([domain]))
                        ip_to_sites.setdefault(ip, set())
                        ip_to_sites[ip] = ip_to_sites[ip].union(set([site]))
            else:
                print(obj['name'], obj['status'])
        except Exception as e:
            print("Exception: ", e)
            exc_type, _, exc_tb = sys.exc_info()
            print(row, exc_type, exc_tb.tb_lineno , "\n\n")

performQueries(domain_to_cdn, domains_to_sites, ip_to_domains, ip_to_sites, cdn_map)

count_unique = 0
with_cdn = 0
potential = 0

with open("../output/unique_"+str(in_name)+"_"+today+".txt", "w") as f:
    for ip in ip_to_sites:
        if len(ip_to_sites[ip]) == 1:
            # here the ip is unique
            domains = ip_to_domains[ip]
            for domain in domains:
                count_unique += 1
                try:
                    cdn = domain_to_cdn[domain]
                    with_cdn += 1
                    line = next(iter(ip_to_sites[ip])) + "," + domain + "," +  ip + "," + str(domain_to_resources[domain])+ "," + str(domain_to_cdn[domain]) + "\n"
                    f.write(line)
                except KeyError:
                    potential += 1
                    line = next(iter(ip_to_sites[ip])) + "," + domain + "," +  ip + "," + str(domain_to_resources[domain]) + ", CDN missing \n"
                    f.write(line)
                
    
print("count_unique: ", count_unique)
print("cdn present: ", with_cdn)
print("potential: ", potential)


    



