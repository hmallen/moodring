import configparser

import pandas as pd

from pymongo import MongoClient


class ProcessData:
    def __init__(self, config_path='settings.conf'):
        self.config = configparser.RawConfigParser()
        self.config.read(config_path)

    def create_dateframe(self, query={}, limit=0):
        mongo_client = MongoClient(self.config["mongodb"]["connection_string"])
        mongo_db = mongo_client[self.config["mongodb"]["db"]]
        mongo_coll = mongo_db[self.config["mongodb"]["collection"]]

        docs = mongo_coll.find(query)

        data = {
            'timestamp': [],
            'user_name': [],
            'user_id': [],
            'bot': [],
            'channel': [],
            'message': [],
            'message_id': [],
            'sentiment': []
        }

        for msg in docs:
            data['timestamp'].append(msg['timestamp'])
            data['user_name'].append(msg['author']['name'])
            data['user_id'].append(msg['author']['id'])
            data['bot'].append(msg['author']['isBot'])
            data['channel'].append(msg['channel']['name'])
            data['message'].append(msg['content'])
            data['message_id'].append(msg['id']),
            data['sentiment'].append(None)

        df = pd.DataFrame(data)

        if limit:
            df = df[:limit]

        self.dataframe = df

        return self.dataframe

    def write_data(self, path="data/dataframe.csv"):
        self.dataframe.to_csv(path, index=False)
