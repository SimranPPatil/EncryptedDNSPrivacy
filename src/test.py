import shelve
from pprint import pprint

with shelve.open('domain_to_resources_resource_sample_2019-04-12') as db:
    for k in db:
        pprint(db[k])
