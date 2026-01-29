import os
import sys
import json

from dotenv import load_dotenv
load_dotenv()

MONGO_DB_URL= os.getenv("MONGO_DB_URL")
print(MONGO_DB_URL)

import certifi
ca=certifi.where()

import pandas as pd
import numpy as np
import pymongo
from networksecurity.exception.exception import CustomException
from networksecurity.logging.logger import logging

class NetworkDataExtract():
    def __init__(self):
        try:
            pass
        except Exception as e:
            raise CustomException(e,sys)

    def cv_to_json_convertor(self,file_path):
        try:
            data=pd.read_csv(file_path)
            data.reset_index(drop=True,inplace=True)
            records = json.loads(data.to_json(orient='records'))
            return records
        except Exception as e:
            raise CustomException(e,sys)
        
    def insert_data_to_mongodb(self,records,database,collection):
        try:
            self.database = database
            self.collection = collection
            self.records = records

            self.mongo_client = pymongo.MongoClient(
                MONGO_DB_URL,
                tls=True,
                tlsCAFile=ca
            )

            self.database = self.mongo_client[self.database]
            self.collection = self.database[self.collection]
            self.collection.insert_many(self.records)
            return len(self.records)
        except Exception as e:
            raise CustomException(e,sys)

if __name__=="__main__":
    FILE_PATH = "Network_Data/phisingData.csv"
    DATABASE = "DIVYDB"
    COLLECTION = "NetworkData"
    network_dataobj = NetworkDataExtract()
    RECORDS = network_dataobj.cv_to_json_convertor(FILE_PATH)
    len_of_records=network_dataobj.insert_data_to_mongodb(RECORDS, DATABASE, COLLECTION)
    print(len_of_records)