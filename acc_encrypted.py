import socket
import json
import unicodedata
from difflib import SequenceMatcher

data = []
with open("har_ip_url.json") as f :
    data = json.load(f)

cdn = {}

out = open("manual_encrypted.csv", "w+")
line = "ip,host,url\n"
out.write(line)

# host is the query returned
# url and ip from the json file

def getCDN(host):
    cdn = ""
    if "akamai" in host:
        cdn = "akamai"
    elif "cloudflare" in host:
        cdn = "cloudflare"
    elif "-cloud.net" in host:
        cdn = "cloud.net"
    elif "cloudfront.net" in host:
        cdn = "cloudfront"
    elif "amazonaws" in host:
        cdn = "aws"
    return cdn

def findMatch(host, url):
    print(host, url)
    if "1e100.net" in host:
        if "google" in url:
            return "1e100.net"
    s = SequenceMatcher(None, host, url).find_longest_match(0, len(host), 0, len(url))
    return host[s.a: s.a + s.size]

score = 0
cdn_score = 0
i = 0
print(len(data))
for d in data:
    try:
        ip = ""
        ip = (d[u'ip']).encode('ascii','ignore')[1:-1]
        url = (d[u'url']).encode('ascii','ignore')
        try:
            i += 1
            host = socket.gethostbyaddr(ip)
            CDN = getCDN(host[0])
            if CDN != "":
                cdn.setdefault(ip, []).append(CDN)
                cdn_score += 1
            if len(findMatch(host[0], url)) > 4:
                score += 1
            else:
                # send for manual check
                if CDN == "":
                    line = ip + "," + host[0] + "," + url + "\n"
                    out.write(line)
        except Exception as e:
            print "Unable to resolve host ", ip
            continue
    except KeyError:
        continue

print(cdn)
print(score)
print(i)
print(cdn_score)
print(float(score+cdn_score)/float(i))
