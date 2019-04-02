# EncryptedDNSPrivacy

The project has scripts to analyse the domain to ip mappings of data using datasets fetched from the Http archive.
Big query is used to extract the web crawl data.

acc.py is used to measure the accuracy of reverse dns mapping from ips fetched from the web request payloads to the urls that were contacted.

ipsets.py generates the domains associated with sites as a part of the multiple subqueries, the domain to ip mapping using the zdns package, and the ip frequency to get the number of hits found. 

Link to 1.json: https://drive.google.com/file/d/1QJg2i9r1v7fTga_up8c-B1f1peWWQb4W/view?usp=sharing

This is the input to the initial runs of ipsets.py 
