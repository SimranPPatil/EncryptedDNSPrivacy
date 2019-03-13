import socket
import json
import unicodedata
from difflib import SequenceMatcher
import requests

data = []
with open("pjWithIp10000.json") as f:
    for line in f:
        data.append(json.loads(line))

cdn = {}

out = open("manual.csv", "w+")
line = "ip,host,url\n"
out.write(line)

class RequestObject(object):
    host = ""
    ip = ""
    rdns = ""
    isp = ""

    def __init__(self, host, ip, rdns, isp):
        self.host = host
        self.ip = ip
        self.rdns = rdns
        self.isp = isp

    def __str__(self):
        return str(self.__dict__)

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
'''
def findMatch(host, url, request_url, page_url):
    print(host, url, request_url, page_url)
    if "1e100.net" in host:
        if "google" in url:
            return "1e100.net"
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
'''
# assuming url, request_url and pageUrl are the same
def findMatch(host, url):
    if "1e100.net" in host:
        if "google" in url:
            return "1e100.net"
    s = SequenceMatcher(None, host, url).find_longest_match(0, len(host), 0, len(url))
    return host[s.a: s.a + s.size]

score = 0
cdn_score = 0
i = 0
for d in data:
    try:
        ip = ""
        ip = d['dest_ip'][1:-1]
        url = d['url']
        request_url = d['request_url'][1:-1]
        page_url = d['pageUrl']
        try:
            print(i)
            i += 1
            web = "https://tools.keycdn.com/geo.json?host="+ip
            res = requests.get(str(web)).json()
            if res["status"]  == "success":
                geo = res["data"]["geo"]
                ro = RequestObject(geo["host"], geo["ip"], geo["rdns"], geo["isp"])
                # host = socket.gethostbyaddr(ip)
                # print(host, ip)
                CDN = getCDN(ro.isp.lower()+" "+ro.rdns.lower())
                if CDN != "":
                    cdn.setdefault(ip, []).append(CDN)
                    cdn_score += 1
                if len(findMatch(ro.rdns, url)) > 4:
                    score += 1
                else:
                    # send for manual check
                    if CDN == "":
                        line = ip + "," + ro.rdns + "," + url + "\n"
                        out.write(line)
            else:
                print(res["status"])
        except Exception as e:
            print("Unable to resolve host ", ip)
            continue
    except KeyError:
        continue

print(cdn)
print(score)
print(i)
print(cdn_score)
print(float(score+cdn_score)/float(i))
