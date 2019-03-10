import socket
import json
import unicodedata
from difflib import SequenceMatcher

data = []
with open("pjWithIp10000.json") as f:
    for line in f:
        data.append(json.loads(line))

cdn = {}

out = open("manual.csv", "w+")
line = "ip,host,url\n"
out.write(line)

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

def findMatch(host, url, request_url, page_url):
    if "1e100.net" in host:
        if "google" in url:
            return len("1e100.net")
    s_pageurl = SequenceMatcher(None, host, page_url).find_longest_match(0, len(host), 0, len(page_url))
    s_url = SequenceMatcher(None, host, url).find_longest_match(0, len(host), 0, len(url))
    s_requrl = SequenceMatcher(None, host, request_url).find_longest_match(0, len(host), 0, len(request_url))
    s = ""
    if s_pageurl.size > s_url:
        if s_pageurl.size > s_requrl.size:
            s = s_pageurl
        else:
            s = s_requrl
    else:
        if s_url.size > s_requrl.size:
            s = s_url
        else:
            s = s_requrl

    return host[s.a: s.a + s.size]

score = 0
cdn_score = 0
i = 0
for d in data:
    try:
        ip = ""
        ip = (d[u'dest_ip']).encode('ascii','ignore')[1:-1]
        url = (d[u'url']).encode('ascii','ignore')
        request_url = (d[u'request_url']).encode('ascii','ignore')
        page_url = (d[u'pageUrl']).encode('ascii','ignore')
        try:
            i += 1
            host = socket.gethostbyaddr(ip)
            CDN = getCDN(host[0])
            if CDN != "":
                cdn.setdefault(ip, []).append(CDN)
                cdn_score += 1
            if len(findMatch(host[0], url, request_url, page_url)) > 4:
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
