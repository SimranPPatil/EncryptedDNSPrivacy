import os
import sys
import json 
import numpy as np
import pandas as pd
from sklearn.preprocessing import LabelEncoder, OneHotEncoder
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_extraction import FeatureHasher
from sklearn.ensemble import RandomForestRegressor


# Class to extract a Resource Object
class Resource:
  def __init__(self, RequestID, Site, LoadURL, LoadDomain, 
      Type, MimeType, RemoteIPAddr, ModTime):
    self.request_id = RequestID
    self.site = Site
    self.load_url = LoadURL
    self.load_domain = LoadDomain
    self.resource_type = Type
    self.mimetype = MimeType
    self.ip_addr = RemoteIPAddr
    self.mod_time = ModTime

# Function to parse and create Resources from JSON
def extract_from_JSON(filename):
    ipaddresses = set()
    resources = []
    website_to_ipset = dict()
    with open(filename) as f:
        for line in f:
            data = json.loads(line)
            # create set of all IPs seen
            resource = Resource(
                data["RequestID"], data["Site"], data["LoadURL"], 
                data["LoadDomain"], data["Type"], data["MimeType"], data["RemoteIPAddr"], data["ModTime"])
            ipaddresses.add(resource.ip_addr)

            if resource.site not in website_to_ipset:
                website_to_ipset.setdefault(resource.site, set())
            website_to_ipset[resource.site].add(resource.ip_addr)
            resources.append(resource)
    
    return list(ipaddresses), resources, website_to_ipset

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("python3 classifier.py path_to_json")
        exit()

    # Pandas to create dataframes
    json_data = pd.read_json(sys.argv[1], lines=True)

    fh = FeatureHasher(n_features=20, input_type='string')
    hashed_features = fh.fit_transform(json_data['RemoteIPAddr'])
    hashed_features = hashed_features.toarray()
    labels = json_data[['Site']]
    features = pd.DataFrame(hashed_features)
    
    # Split the data into test and train randomly (80-20)
    train_features, test_features, train_labels, test_labels = train_test_split(features, labels, test_size=0.2)

    '''
    # Unique labels for ip_addresses
    le = LabelEncoder()
    ip_labels = le.fit_transform(json_data['RemoteIPAddr'])
    ip_mappings = {index: label for index, label in 
                    enumerate(le.classes_)}
    json_data['ip_labels'] = ip_labels
    # print("ip_mappings: " , ip_mappings)

    # OneHot Encoding the data
    ohe = OneHotEncoder(categories='auto')
    ip_feature_arr = ohe.fit_transform(json_data[['ip_labels']]).toarray()
    ip_feature_labels = list(le.classes_)
    ip_features = pd.DataFrame(ip_feature_arr, columns=ip_feature_labels)

    df_sub = json_data[['RemoteIPAddr', 'ip_labels', 'Site']]
    df_ohe = pd.concat([df_sub, ip_features], axis=1)
    columns = sum([["RemoteIPAddr", "ip_labels", "Site"], ip_feature_labels],[])
    print(df_ohe[columns].iloc[4:10])
    
    ###############################################################################

    onehot_features = pd.get_dummies(json_data['RemoteIPAddr'])
    df = pd.concat([json_data[['RemoteIPAddr']], onehot_features, json_data[['Site']]], axis=1)
    
    # Split the data into test and train randomly (80-20)
    train, test = train_test_split(df, test_size=0.2)
    '''
    print(train_features.shape, test_features.shape)
    # Instantiate model with 1000 decision trees
    clf = RandomForestClassifier(max_depth=5, n_estimators=10)
    clf.fit(train_features, train_labels.values.ravel())
    print(clf.score(test_features, test_labels.values.ravel()))
    pass