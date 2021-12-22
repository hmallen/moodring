import argparse
import configparser
import dateutil
import json
import logging
import os
import sys

from pymongo import MongoClient


logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def gather_users():
    pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i",
        "--input",
        type=str,
        help="Path to input as a json file or directory containing json files.",
        default='data/'
    )
    parser.add_argument(
        "-c", "--config", type=str, default="config.ini", help="Path to config file."
    )
    args = parser.parse_args()

    config_file = args.config
    input_path = args.input

    config = configparser.RawConfigParser()
    config.read(config_file)

    mongo_client = MongoClient(config["mongodb"]["connection_string"])
    mongo_db = mongo_client[config["mongodb"]["db"]]
    mongo_coll = mongo_db[config["mongodb"]["collection"]]

    inputs = []
    if ".json" in input_path:
        inputs.append(input_path)

    else:
        for file in os.listdir(input_path):
            if ".json" in file:
                inputs.append(file)

    for json_file in inputs:
        with open(json_file, "r") as file:
            data = json.load(file)

        guild = data["guild"]
        channel = data["channel"]
        for msg in data["messages"]:
            msg["guild"] = guild
            msg["channel"] = channel

            msg["timestamp"] = dateutil.parser.parse(msg["timestamp"])

            result = mongo_coll.update_one(
                {"id": msg["id"]}, {"$set": msg}, upsert=True
            )
            logger.debug(f"result.upserted_id: {result.upserted_id}")

    print("Done.")
