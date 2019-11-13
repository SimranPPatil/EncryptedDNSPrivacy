This script enables the user to do the following:
- Generates the table that has site_url, load_url, site_domain, load_domain, site_cdn, load_cdn, resource_type, site_ip, load_ip
- Fetches a domain list and maps the domains to the set of ips 
- Stores the table in BigQuery 

Project: ipprivacy
Dataset: subsetting
Table: create the table <supply: 2019_10_01_desktop>

run the script --> python3 pipeline.py 2019_10_01_desktop


