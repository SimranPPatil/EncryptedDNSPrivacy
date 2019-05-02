# EncryptedDNSPrivacy

The project has scripts to analyse the domain to ip mappings of data using datasets fetched from the Http archive.
Big query is used to extract the web crawl data.
The goal is to study the privacy implications of encryption mechanisms in the web particularly because of the visibility of destination IP addresses as a part of TLS1.3 handshake's ClientHello. 

acc.py is used to measure the accuracy of reverse dns mapping from ips fetched from the web request payloads to the urls that were contacted.

ipsets.py generates the domains associated with sites as a part of the multiple subqueries, the domain to ip mapping using the zdns package, and the ip frequency to get the number of hits found. 

Link to 1.json: https://drive.google.com/file/d/1QJg2i9r1v7fTga_up8c-B1f1peWWQb4W/view?usp=sharing

This is the input to the initial runs of ipsets.py

The scripts involved also work with finding ip mapping between domains and websites and the resources hosted by them and the CDNs associated, if any.

bq.py utilises Google's BigQuery API to fetch data from HTTP archive and generate the data set we need. 

parse_csv.py does a similar kind of processing with the data fetched from Illinois's local server.

parse_disk.py works with parsing large data files while utilising persistent storage for storing the massive data structures generated on the go rather than doing the processing in memory. 

parse_output.py is used to get a graphical visualisation of the output files generated. 

 
