#!/usr/local/bin/python3.6
import json
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from json import dumps
import os, uuid
import time
from threading import  Thread
from mimesis import *
import sys
import logging


#Defining variables
person = Person()
food = Food()
unit_system = UnitSystem()
hardware = Hardware()
internet = Internet()
file = File()
address = Address()


# this variable will be used with the index name
date_today = datetime.today().strftime('%Y-%m-%d-%H.%M.%S')

# take input of elasticsearch IP and Port
host = "localhost"
port = 9200
number_of_documents = 0
# connect to the elasticsearch
client=Elasticsearch([{'host':host,'port':port}])



def enable_stdout(value):
    if (value):
        # log everything to stdout
        root = logging.getLogger()
        root.setLevel(logging.DEBUG)
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        root.addHandler(handler)




class Fake_data:

    # open file for storing some info
    logfile = open("log.txt", "a")

    #create number of ojbects for Person
    def person_data(self,length):
        for i in range(length):
            yield     {
            #"_id": str(uuid.uuid4()),
            "_index": "person."+date_today,
            "doc": {
                "last_name": person.surname(),
                "first_name": person.name(),
                "Academic_degree": person.academic_degree(),
                "email": person.email()
            }
        }

    #create number of ojbects for Address
    def address_data(self,length):
        for i in range(length):
            yield     {
            #"_id": str(uuid.uuid4()),
            "_index": "address."+date_today,
            "doc": {
                "address": address.address(),
                "country_code": address.country_code(),
                "country": address.country(),
                "continent":address.continent(),
                "city":address.city(),
                "current_locale":address.get_current_locale(),
                "coordinates":address.coordinates(),
                "latitude":address.latitude(),
                "longitude": address.longitude()
            }
        }


    def file_data(self,length):
        for i in range(length):
            yield     {
            #"_id": str(uuid.uuid4()),
            "_index": "file."+date_today,
            "doc": {
                "file_name": file.file_name(),
                "size": file.size(),
                "mime_type": file.mime_type(),
                "extension":file.extension(),
            }
        }

    def hardware_data(self,length):
        for i in range(length):
            yield     {
            #"_id": str(uuid.uuid4()),
            "_index": "hardware."+date_today,
            "doc": {
                "cpu": hardware.cpu(),
                "frequency": hardware.cpu_frequency(),
                "codename": hardware.cpu_codename(),
                "model_code":hardware.cpu_model_code(),
                "generation":hardware.generation(),
                "manufacturer":hardware.manufacturer(),
                "graphics":hardware.graphics(),
                "phone_model":hardware.phone_model(),
                "ram_size": hardware.ram_size(),
                "ram_type": hardware.ram_type(),
                "resolution": hardware.resolution(),
                "ssd_or_hdd": hardware.ssd_or_hdd(),
                "screen_size": hardware.screen_size()
            }
        }

    def internet_data(self,length):
        for i in range(length):
            yield     {
            #"_id": str(uuid.uuid4()),
            "_index": "internet."+date_today,
            "doc": {
                "content_type": internet.content_type(),
                "ip": internet.ip_v4(),
                "ip_v6": internet.ip_v6(),
                "emoji":internet.emoji(),
                "home_page":internet.home_page(),
                "network_protocl":internet.network_protocol(),
                "mac_address":internet.mac_address(),
                "user_agent":internet.user_agent(),
                "port": internet.port(),
                "http_method": internet.http_method(),
                "http_status_code": internet.http_status_code(),
                "http_status_message": internet.http_status_message()

            }
        }


    def unit_system_data(self,length):
        for i in range(length):
            yield     {
            #"_id": str(uuid.uuid4()),
            "_index": "unit_system."+date_today,
            "doc": {
                "unit": unit_system.unit(),
                "prefix": unit_system.prefix(),
            }
        }

    def food_data(self,length):
        for i in range(length):
            yield     {
            #"_id": str(uuid.uuid4()),
            "_index": "food."+date_today,
            "doc": {
                "vegetable": food.vegetable(),
                "fruit": food.fruit(),
                "dish": food.dish(),
                "drink":food.drink(),
                "spices":food.spices(),
                "current_locale":food.get_current_locale(),
            }
        }

    def generate_all_types(self,length=500000):
        start = time.time()
        try:
            Thread(target=helpers.bulk, args=[client, [i for i in self.person_data(length)]]).start()
            Thread(target=helpers.bulk, args=[client, [i for i in self.internet_data(length)]]).start()
            Thread(target=helpers.bulk, args=[client, [i for i in self.file_data(length)]]).start()
            Thread(target=helpers.bulk, args=[client, [i for i in self.food_data(length)]]).start()
            Thread(target=helpers.bulk, args=[client, [i for i in self.hardware_data(length)]]).start()
            Thread(target=helpers.bulk, args=[client, [i for i in self.unit_system_data(length)]]).start()
            Thread(target=helpers.bulk, args=[client, [i for i in self.address_data(length)]]).start()


            end =time.time() - start

            self.logfile.write(date_today+" : The proess to bulk "+ str(length*7) + " documents took "+ str(end) + " seconds"+'\n')
            self.logfile.close()
        except Exception as e:
            self.logfile.write(date_today+ " : "+ str(e)+"\n")
            self.logfile.close()



class __main__:
    def main():
        global host
        global port
        global client
        global number_of_documents

        if len(sys.argv) >= 3:
            host = sys.argv[1]
            port = sys.argv[2]
            number_of_documents = sys.argv[3]
            with_stdout = sys.argv[4].lower() == 'true'

        enable_stdout(with_stdout)
        client=Elasticsearch([{'host':host,'port':port}])
        generator = Fake_data()
        generator.generate_all_types(int(number_of_documents))

    if __name__ == "__main__":
        main()