import json
from pprint import pprint

data = {}
results = []
with open("new.json") as f :
    data = json.load(f)

# pprint(data)
# pprint(data["log"]["version"])
# print data["log"]["entries"][0]["startedDateTime"]

for elem in data["log"]["entries"]:
    url = elem["request"]["url"]
    ip = ""
    if elem.has_key("serverIPAddress"):
        ip = elem["serverIPAddress"]
    obj = {"url": url, "ip": ip}
    results.append(obj)

# for elem in results:
#     print "url: " + elem["url"] + "\n" + "ip: " + elem["ip"]

with open("har_ip_url_new.json", "w") as f:
    json.dump(results, f)
