# global python packages
import copy
import json
import math
import os

import requests
import sys
import time

# local files
from local_tools import is_dict, is_dictkey

# technically you could replace every instance of is_dict with 'isinstance(variable, dict)'

# global variables for later use
# probably bad practice on my part
MAIN_URL = ""
MAIN_URL_PARAM = {}
CHUNK_SIZE = 50
ERROR = {}
STRUCTURE = {}
statistic = {}
start_time = time.time()  # time for execution time


# load config data

# error message handler, requires load config before, else it will do just send message
def send_error(message, error_name=None):
    global ERROR
    if error_name is None:
        sys.stderr.write(message)
    else:
        if is_dictkey(ERROR, error_name):
            sys.stderr.write(ERROR[error_name].format(message))
        else:
            sys.stderr.write(message)


def load_config(file_path="settings.json"):
    global MAIN_URL, MAIN_URL_PARAM, ERROR, STRUCTURE, CHUNK_SIZE
    with open(file_path) as json_file:
        data = json.load(json_file)
        try:
            ERROR = data['errors']
        except KeyError:
            send_error("Cannot find 'error' Listings in {} File".format(file_path))  # in this there is not error field
            sys.exit()

        try:
            STRUCTURE = data['structure']
        except KeyError:
            send_error("structure", "key")
            sys.exit()

        if is_dictkey(data, "url"):
            MAIN_URL = data['url']
            MAIN_URL_PARAM = data['url_para']

        if is_dictkey(data, "chunk_size"):
            CHUNK_SIZE = int(data['chunk_size'])


# directly requests from solr
def load_remote_content(url, params):
    time1 = time.time()
    try:
        resp = requests.get(url, params=params)
        print("Trying URL {}".format(resp.url))
        print("Request Code {}".format(resp))
        print("Header: \n {}".format(resp.headers))

        # print("Content:"
        print("Remote load took {} seconds".format(str(round(time.time() - time1, 3))))
        return resp.text
    except requests.exceptions.RequestException as e:
        send_error(e, "file")


def test_json(json_str):
    global ERROR
    try:
        data = json.loads(json_str)
        return data
    except ValueError:
        print(ERROR['json'])
        return False


def traverse_json_data(data):
    # all this does ist to traverse the json to the point where the entries are
    # which is in this hardcoded case the second level in a structure defined by
    # the setting structure['content'], in my cases its called "docs"
    global STRUCTURE
    i = 0
    for (key, value) in data.items():
        if key == STRUCTURE['header']:
            header = value
            continue
        elif key == STRUCTURE['body'] and is_dict(value):  # next iteration, we know this is a dataset
            for (key2, value2) in value.items():
                if key2 != STRUCTURE['content']:
                    continue
                else:
                    for value3 in enumerate(value2):
                        add_entry(value3)


debug_json = []


def add_entry(entry):
    global statistic, ERROR, debug_json
    try:
        for key, value in entry[1].items():  # tuples start at 1, urg
            # the actual entries are actually tuples and dont really possess a key and value like a dict does
            if is_dictkey(statistic, key):
                statistic[key] += 1
            else:
                statistic[key] = 1
            # print("{} = {}".format(key, str(value)))
        debug_json.append(entry[1])
    except TypeError:
        send_error("add_entry", "typeerror")


def write_statistic(file_path="stats.json"):
    # this functions seems rather redundant, but it makes main a bit prettier
    global statistic
    try:
        with open(file_path, 'w') as outfile:
            json.dump(statistic, outfile, indent=4)

        print("{} Bytes of Data were written to {}".format(os.stat(file_path).st_size, file_path))
    except FileExistsError:
        send_error("", "FileExistsError")
    except FileNotFoundError:
        send_error("", "FileNotFoundError")


def main():
    load_config()  # default path should suffice
    # mechanism to not load 50000 entries in one go but use chunks for it
    n = math.floor(int(MAIN_URL_PARAM.get('rows')) / CHUNK_SIZE) + 1
    temp_url_param = copy.deepcopy(MAIN_URL_PARAM)  # otherwise dicts get copied by reference
    for i in range(0, n):
        temp_url_param['start'] = i*CHUNK_SIZE
        print("New Chunk started: [{}/{}] - {} ms".format(i, n-1, round(time.time(), 2)))
        if i+1 != n:
            temp_url_param['rows'] = CHUNK_SIZE
        else:
            print(MAIN_URL_PARAM.get('rows'))
            temp_url_param['rows'] = int(int(MAIN_URL_PARAM.get('rows')) % CHUNK_SIZE)
        print(temp_url_param)
        temp_json = load_remote_content(MAIN_URL, temp_url_param)
        data = test_json(temp_json)
        if data:  # no else required, test_json already gives us an error if something fails
            traverse_json_data(data)
            write_statistic()


if __name__ == "__main__":
    main()
    print("Overall Executiontime was {} seconds".format(str(round(time.time() - start_time, 3))))
    print("Statistic has {} entries".format(len(statistic)))
    with open("debug.json", "w") as fw:
        json.dump(debug_json, fw, indent=4)