import argparse
import configparser
import datetime
import dateutil.parser
import json
import logging
import os
import sys

from pymongo import MongoClient

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

parser = argparse.ArgumentParser()
parser.add_argument(
    "-i",
    "--input",
    type=str,
    default='data/',
    help="Path to input as a json file or directory containing json files."
)
parser.add_argument(
    "-c", "--config", type=str, default="settings.conf", help="Path to config file."
)
parser.add_argument(
    '-y', '--yes', action="store_true", default=False, help="Assume yes to confirmation prompt."
)
args = parser.parse_args()

input_path = args.input
config_file = args.config
confirm_prompt = args.yes

config = configparser.RawConfigParser()
config.read(config_file)


if __name__ == "__main__":
    mongo_client = MongoClient(config["mongodb"]["connection_string"])
    mongo_db = mongo_client[config["mongodb"]["db"]]
    mongo_coll = mongo_db[config["mongodb"]["collection"]]

    inputs = []
    if ".json" in input_path:
        inputs.append(input_path)

    else:
        for file in os.listdir(input_path):
            if ".json" in file:
                inputs.append(f"{input_path}/{file}")

    if not confirm_prompt:
        print('\nReady to import the following files.\n')
        [print(file) for file in inputs]
        user_confirm = input('\nContinue? [y/N]: ')
        if user_confirm.lower() == 'y':
            print('Beginning import.')
        else:
            print('Aborted by user.')
            sys.exit()

    for json_file in inputs:
        with open(json_file, "r") as file:
            data = json.load(file)

        guild = data["guild"]
        channel = data["channel"]
        for count, msg in enumerate(data["messages"]):
            msg["guild"] = guild
            msg["channel"] = channel

            msg["timestamp"] = dateutil.parser.isoparse(msg["timestamp"])

            if msg["timestampEdited"]:
                msg["timestampEdited"] = dateutil.parser.isoparse(
                    msg["timestampEdited"])

            result = mongo_coll.update_one(
                {"id": msg["id"]}, {"$set": msg}, upsert=True
            )
            logger.debug(f"result.upserted_id: {result.upserted_id}")
            logger.info(f"File: {json_file}, Message: {count + 1}")

    print("Done.")
