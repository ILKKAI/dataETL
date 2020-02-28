# -*- coding: utf-8 -*-
import csv
import os
import time

COUNT = 0

CHECK_LIST = list()

TIME_ARRARY = time.localtime()

TIME = time.strftime("%Y-%m-%d")

DIR_PATH = "./"

FILE_PATH = f"{DIR_PATH}{TIME}.csv"


def to_csv(data, file_path=None):
    if not file_path:
        file_path = FILE_PATH

    if not CHECK_LIST:
        for each_key in data:
            CHECK_LIST.append(each_key)

    global COUNT
    COUNT += 1
    if os.path.exists(file_path):
        with open(file_path, "a", newline="", errors="ignore") as f:
            writer = csv.writer(f)
            append_list = list()
            for key in CHECK_LIST:
                try:
                    append_list.append(data[key])
                except KeyError:
                    append_list.append("")
            writer.writerows([append_list])
    else:
        try:
            with open(file_path, "a", newline="", errors="ignore") as f:
                writer = csv.writer(f)
                check_list = list()
                append_list = list()
                for key in CHECK_LIST:
                    check_list.append(key)
                    try:
                        append_list.append(data[key])
                    except KeyError:
                        append_list.append("")
                writer.writerows([check_list, append_list])
        except FileNotFoundError:
            os.makedirs(DIR_PATH)
            with open(file_path, "a", newline="", errors="ignore") as f:
                writer = csv.writer(f)
                check_list = list()
                append_list = list()
                for key in CHECK_LIST:
                    check_list.append(key)
                    try:
                        append_list.append(data[key])
                    except KeyError:
                        append_list.append("")
                writer.writerows([check_list, append_list])


if __name__ == '__main__':
    url_dict = dict()
    with open("common_bidding1.csv", "r", newline="", errors="ignore") as f:
        reader = csv.reader(f)
        for r in reader:
            if r[0] == "ID_":
                pass
            else:
                url_dict[r[0]] = r[17]
    print(url_dict)
