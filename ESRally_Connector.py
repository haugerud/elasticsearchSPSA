import json
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from json import dumps
import os, uuid
import time
from threading import  Thread
import requests



class ESRally_connector(object):
    datetime=datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    dir_name = "rally"+ str(datetime)
    command = ""
    def __init__(self, elasticsearch_node):
        self.elasticsearch_node = elasticsearch_node
        self.command = "sudo docker run --rm -v $PWD/"+self.dir_name+":/rally/.rally elastic/rally --track=nyc_taxis --test-mode --pipeline=benchmark-only --target-hosts="+self.elasticsearch_node
        #128.39.120.25:9200

    def get_race_json(self):
        print("Creating new directory to store the test ...")
        os.system('mkdir '+ self.dir_name)
        os.system('sudo chgrp 0 $PWD/'+self.dir_name)
        os.system(self.command)
        os.system(self.command)
        f = [(os.getcwd()+"/"+self.dir_name+"/benchmarks/races/"+dI) for dI in os.listdir(self.dir_name+'/benchmarks/races/') if os.path.isdir(os.path.join(self.dir_name+'/benchmarks/races/',dI))]
        os.chdir(str(f[0]))
        print('path of race.json : '+str(f[0])+"/race.json")
        return str(f[0])+"/race.json"

    def update_node_settings(self,RF,TS,SI,RMB):
    ## in future, make it read a list
        referesh_interval = "5"
        url = "http://"+self.elasticsearch_node+"/_settings"
        data = {'index' : {'refresh_interval' : RF,'translog' : {"flush_threshold_size": TS, "sync_interval": SI }}}
        headers = {'Content-type': 'application/json'}
        r = requests.put(url, data=json.dumps(data), headers=headers)

        ##updating cluster parameters
        url_cluster = "http://"+self.elasticsearch_node+"/_cluster/settings"
        data_rmb = {"persistent" : {"indices.recovery.max_bytes_per_sec" : RMB}}
        cluster_request = requests.put(url, data=json.dumps(data), headers=headers)

        print(r)
        print(cluster_request)
