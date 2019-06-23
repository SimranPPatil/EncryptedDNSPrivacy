import sqlite3
import sys
import matplotlib.pyplot as plt

def main():
    sqdb = sqlite3.connect(sys.argv[1])
    sqcur = sqdb.cursor()
    ipsetsize2domaincount = dict()
    domain2ipset = dict()
    for row in sqcur.execute('Select domain, ip, count(ip) from domain2ip group by domain order by count(ip)'):
        ipsetsize2domaincount.setdefault(row[2], set()).add(row[0])

    keys = []
    domains = []
    for key in ipsetsize2domaincount:
        keys.append(key)
        domains.append(len(ipsetsize2domaincount[key])) 
    
    plt.bar(keys, domains)
    plt.xlabel('Anonymity set sizes')
    plt.ylabel('# of Domains in ipset size')
    plt.savefig('output/domainsetsize.png')

    for row in sqcur.execute('Select domain, ip from domain2ip'):
        domain2ipset.setdefault(row[0], set()).add(row[1])

    ipset2domains = dict()
    for domain in domain2ipset:
        ipset = domain2ipset[domain]
        ipset2domains.setdefault(frozenset(ipset), set()).add(domain)

    with open('output/domainswithsameipset.txt', "w") as f:
        for ipset in ipset2domains:
            if(len(ipset2domains[ipset])>1):
                f.write(str(ipset) + ", " + str(len(ipset2domains[ipset])) + "\n")
    
    
    with open('output/typecount.txt', "w") as f:
        for row in sqcur.execute('Select type, count(type) as frequency from bq_crawl group by type order by count(type) desc'):
            f.write(row[0] + ", " + str(row[1]) + "\n")

    with open('output/mimetypecount.txt', "w") as f:
        for row in sqcur.execute('Select mimeType, count(mimeType) as frequency from bq_crawl group by mimeType order by count(mimeType) desc'):
            f.write(row[0] + ", " + str(row[1]) + "\n")
    
if __name__ == "__main__":
    main()   