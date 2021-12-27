import sys
import warnings

import pandas as pd

from pymongo import MongoClient

# sys.path.insert(0, ".")
from action_logging import Logger

warnings.filterwarnings("ignore")


class Preprocess:
    def __init__(self, config, logger=None):
        """
        Initialize Preprocess class
        :param config:
        """
        if logger is None:
            self.logger = Logger(
                log_flag=True, log_file="preprocess", log_path="../logs/"
            )
        else:
            self.logger = logger
        self.users = None
        self.color_map = {}
        self.data = None
        self.data_backup = None
        self.pd_data = None

        mongo_client = MongoClient(config["mongo"]["connection_uri"])
        mongo_db = mongo_client[config["mongo"]["db"]]
        self.mongo_collection = mongo_db[config["mongo"]["collection"]]

    def load_data(self):
        """
        Reads in the array of dicts in UTF-8, parses, and formats
        Also, takes backup just to have control on things.
        :return:
        """
        self.logger.write_logger("In preprocess.py (load_data): Loading data.")

        mongo_data = self.mongo_collection.find({})

        # Save temporary value
        self.data_backup = self.data.copy()

        self.logger.write_logger("In preprocess.py (load_data): Data load complete.")

    def print_sample(self, n_lines=10):
        """
        Prints sample number of lines
        :param n_lines:
        :return:
        """
        self.logger.write_logger(
            "In preprocess.py (print_sample): Printing of data (first "
            + str(n_lines)
            + " lines) starts"
        )
        print(self.data[:n_lines])
        self.logger.write_logger(
            "In preprocess.py (print_sample): Printing of data (first "
            + str(n_lines)
            + " lines) ends"
        )

    def drop_message(self, contains):
        """
        Drops the message if it contains the text given in parameter
        :param contains:
        :return:
        """
        self.logger.write_logger(
            "In preprocess.py (drop_message): Dropping message containing: "
            + contains
            + " starts"
        )

        self.data = [line for line in self.data if contains not in line]

        self.logger.write_logger(
            "In preprocess.py (drop_message): Dropping message containing: "
            + contains
            + " ends"
        )
        return self

    def prepare_df(self):
        """
        Prepares a Pandas Dataframe out of the data
        :return:
        """
        timestamps = []
        ts_first_split = []  # stores first split of xx/xx/xx, xx:xx xx
        users = []
        messages = []
        self.logger.write_logger(
            "In preprocess.py (prepare_df): Preparation of data frame starts"
        )

        for line in self.data:
            timestamps.append(line.split("-")[0].strip())
            ts_first_split.append(int(line.split("-")[0].strip().split("/")[0].strip()))
            sub_line = line.split("-")[1].strip().split(":")
            users.append(sub_line[0].strip())
            messages.append("-".join([v.strip() for v in sub_line[1:]]))
        self.pd_data = pd.DataFrame(
            {"Timestamp": timestamps, "User": users, "Message": messages}
        )[["Timestamp", "User", "Message"]]
        if sum([1 if v > 12 else 0 for v in ts_first_split]) > 0:
            self.pd_data["Timestamp"] = pd.to_datetime(
                self.pd_data["Timestamp"].str.lower(), format="%d/%m/%y, %I:%M %p"
            )
        else:
            self.pd_data["Timestamp"] = pd.to_datetime(
                self.pd_data["Timestamp"].str.lower(), format="%m/%d/%y, %I:%M %p"
            )
        self.pd_data["Date"] = self.pd_data["Timestamp"].dt.strftime("%d-%b-%Y")
        self.pd_data["Weekday"] = self.pd_data["Timestamp"].dt.strftime("%a")
        self.users = list(set(users))
        self.logger.write_logger(
            "In preprocess.py (prepare_df): Preparation of data frame ends"
        )

    def check_n_users(self):
        """
        Check the number of users
        if > 2 or < 2, raise Exception
        :return:
        """
        users = list(set(self.pd_data["User"].tolist()))
        n_users = len(users)
        if n_users != 2:
            self.logger.write_logger(
                "In preprocess.py (check_n_users): You need to have 2 users in the chat. Not more, Not less !",
                error=True,
            )
            self.logger.write_logger(
                f"In preprocess.py (check_n_users): You have {n_users} Users in the chat",
                error=True,
            )
            self.logger.write_logger(
                f"In preprocess.py (check_n_users): {','.join(users)} are the users",
                error=True,
            )
            sys.exit()
        else:
            self.color_map = {
                self.users[0]: "#e74c3c",  # red
                self.users[1]: "#3498db",  # blue
            }
            self.logger.write_logger(
                "In preprocess.py (check_n_users): You Chat data have 2 users.",
                error=False,
            )
            self.logger.write_logger(
                "In preprocess.py (check_n_users): Added the Hex color codes too.",
                error=False,
            )

    def remove_forward_messages(self, min_length=15):
        """
        If number of consecutive messages are > 15 with same timestamp from same user, remove all of them
        :param min_length:
        :return:
        """
        # self.pd_data['Is Forward'] = False
        message_count = (
            self.pd_data.groupby(["User", "Timestamp"])["Message"].count().reset_index()
        )
        messages_to_remove = message_count[message_count["Message"] > min_length][
            ["User", "Timestamp"]
        ]
        messages_to_remove["Is Forward"] = True
        self.pd_data = self.pd_data.merge(
            messages_to_remove, on=["User", "Timestamp"], how="left"
        )
        self.pd_data["Is Forward"] = self.pd_data["Is Forward"].fillna(False)
        self.pd_data = self.pd_data[~self.pd_data["Is Forward"]]
        return self

    def write_data(self, path="data/clean_data.csv"):
        """

        :param path:
        :return:
        """
        self.pd_data.to_csv(path, index=False)
