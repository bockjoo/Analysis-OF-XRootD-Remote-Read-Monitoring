import numpy as np
#import pandas as pd
import matplotlib.pyplot as plt

import os
import sys, json

import time

from sklearn.preprocessing import LabelEncoder
from datetime import datetime,timedelta,date

import requests, json, os
from itertools import groupby

shortURL = "https://monit-opensearch.cern.ch/dashboards/goto/e0b026c5a3dbb183ceb31062397f7627?security_tenant=global"
# Long URL https://monit-opensearch.cern.ch/dashboards/app/discover#/?_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:now-15m,to:now))&_a=(columns:!(_type,data.end_time,data.server_site),filters:!(),index:AWnEUpm3NZZoUCd3kmbV,interval:auto,query:(language:kuery,query:''),sort:!())

#def get_rucio_transfers (dbid=9269, size=10000, gte="now-30m/m",lte="now",unique_f = "metadata.timestamp", tie_breaker_id = "data.request_id"):

os.environ['GRAFANA_VIEWER_TOKEN']='eyJrI_you_should_get_your_own_token_from_the_CMS_Monitoring_Group'


def get_crab_jobs (dbid=9231, size=10000, gte="now-30m/m",lte="now",
                         unique_f = "metadata.timestamp", 
                         tie_breaker_id="metadata.kafka_timestamp"):
    
    url = "https://monit-grafana.cern.ch/api/datasources/proxy/"+str(dbid)+"/_msearch"

    index_name = "monit_prod_cms_rucio_raw_events*" # RUCIO Transfers
    index_name = "monit_prod_cms_raw_aaa-ng*"       # XRD Collector Monitoring
    index_name = "monit_prod_condor_raw_overview*"

    payload_index_props = {
        "search_type": "query_then_fetch",
        "index": [ index_name ],
        "ignore_unavailable":True
    }

    # See /opt/cms/services/HammerCloudXrootdMonitoring/monit_prod_cms_rucio_raw_events_source_dest_sort_search.json.in
    payload_query = {
        "query": {
            "bool": {
                "filter": [
                    {
                        "range": {
                            "metadata.timestamp": {
                                "gte": "@@gte@@",
                                "lte": "@@lte@@",
                                "format": "epoch_millis"
                            }
                        }
                    },
                    {
                        "query_string": {
                            "analyze_wildcard":True,
                            "query": "data.Site: *"
                        }
                    }
                ]
            }
        },
        "from": 0,
        "size": 10000,
        "sort": []
    }

    payload_query["query"]["bool"]["filter"][0]["range"]["metadata.timestamp"]["gte"] = gte
    payload_query["query"]["bool"]["filter"][0]["range"]["metadata.timestamp"]["lte"] = lte
    payload_query["sort"].append({unique_f : "desc"})
    payload_query["sort"].append({tie_breaker_id : "asc"})

    #print (payload_query)


    payload = json.dumps(payload_index_props) + " \n" + json.dumps(payload_query) + "\n"
    headers = {
      'Content-Type': 'application/json',
      'Authorization': 'Bearer {}'.format(os.environ["GRAFANA_VIEWER_TOKEN"])
    }
    result = requests.request("POST", url, headers=headers, data = payload).json()
    results = []
    results.append(result)
    nhits = result["responses"][0]["hits"]["total"]["value"]
    i=0
    #print (i,nhits," vs ",len(result["responses"][0]["hits"]["hits"]), " Searching next hits...", )
    while len(result["responses"][0]["hits"]["hits"]) == size :
       i = i + 1
       #print (i," Searching next hits...")
       payload_query["search_after"] = result["responses"][0]["hits"]["hits"][nhits-1]["sort"]
       payload = json.dumps(payload_index_props) + " \n" + json.dumps(payload_query) + "\n"
       result = requests.request("POST", url, headers=headers, data = payload).json()
       results.append(result)
       nhits = result["responses"][0]["hits"]["total"]["value"]
       #print (i,nhits," vs ",len(result["responses"][0]["hits"]["hits"]), " Searching next hits...", )
    return results
    
dbid=9236
size = 10000
#gte = "now-45m/m"
#gte = "now-6h/h"
gte = "now-1h/h"
lte = "now"
unique_f = "metadata.timestamp"
tie_breaker_id="metadata.kafka_timestamp"
results = get_crab_jobs (dbid, size, gte ,lte, unique_f, tie_breaker_id)
print (results)
