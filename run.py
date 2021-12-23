import argparse
import configparser
import dateutil
import json
import logging
import os
import sys

import pandas as pd
from pymongo import MongoClient


logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

parser = argparse.ArgumentParser()
parser.add_argument(
    "-c", "--config", type=str, default="config.ini", help="Path to config file."
)
args = parser.parse_args()

config_file = args.config

config = configparser.RawConfigParser()
config.read(config_file)

df_header = [
    ('timestamp', 'timestamp'),
    ('user_name', 'author.name'),
    ('user_id', 'author.id'),
    ('channel', 'channel.name'),
    ('message', 'content'),
    ('message_id', 'id')
]


def create_dateframe(query={}):
    mongo_client = MongoClient(config["mongodb"]["connection_string"])
    mongo_db = mongo_client[config["mongodb"]["db"]]
    mongo_coll = mongo_db[config["mongodb"]["collection"]]

    docs = mongo_coll.find(query)

    data = {
        'timestamp': [],
        'user_name': [],
        'user_id': [],
        'channel': [],
        'message': [],
        'message_id': []
    }

    for msg in docs:
        data['timestamp'].append(msg['timestamp'])
        data['user_name'].append(msg['author']['name'])
        data['user_id'].append(msg['author']['id'])
        data['channel'].append(msg['channel']['name'])
        data['message'].append(msg['content'])
        data['message_id'].append(msg['id'])

    df = pd.DataFrame(data)

    return df


if __name__ == "__main__":
    dataframe = create_dateframe(query={})
