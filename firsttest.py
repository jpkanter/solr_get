# global python packages
import json
import os

import requests
import sys
import time

# local files
from local_tools import is_dict, is_dictkey

# technically you could replace every instance of is_dict with 'isinstance(variable, dict)'

# global variables for later use
# probably bad practice on my part
main_url = ""
main_url_param = {}
error = {}
structure = {}
statistic = {}
start_time = time.time() # time for execution time

# load config data

def load_config(file_path="settings.json"):
    global main_url, main_url_param, error, structure
    with open(file_path) as json_file:
        data = json.load(json_file)
        try: error = data['errors']
        except KeyError:
            print("Cannot find 'error' Listings in {} File".format(file_path))
            sys.exit()

        try: structure = data['structure']
        except KeyError:
            print(error['key'].format("structure"))
            sys.exit()

        if is_dictkey(data, "url"):
            main_url = data['url']
            main_url_param = data['url_para']


# directly requests from solr
def load_remote_content(url, params):
    time1 =  time.time()
    try:
        resp = requests.get(url, params=params)
        print("Trying URL {}".format(resp.url))
        print("Request Code {}".format(resp))
        print("Header: \n {}".format(resp.headers))

        # print("Content:"
        print("Remote load took {} seconds".format(str(round(time.time() - time1, 3))))
        return resp.text
    except requests.exceptions.RequestException as e:
        print(error['file'].format(e))


def test_json(json_str):
    global error
    try:
        data = json.loads(json_str)
        return data
    except ValueError:
        print(error['json'])
        return False


def traverse_json_data(data):
    # all this does ist to traverse the json to the point where the entries are
    # which is in this hardcoded case the second level in a structure defined by
    # the setting structure['content'], in my cases its called "docs"
    global structure
    i = 0
    for (key, value) in data.items():
        if key == structure['header']:
            header = value
            continue
        elif key == structure['body'] and is_dict(value):  # next iteration, we know this is a dataset
            for (key2, value2) in value.items():
                if key2 != structure['content']:
                    continue
                else:
                    for value3 in enumerate(value2):
                        add_entry(value3)


def add_entry(entry):
    global statistic, error
    try:
        for key, value in entry[1].items():  # tuples start at 1, urg
            # the actual entries are actually tuples and dont really possess a key and value like a dict does
            if is_dictkey(statistic, key):
                statistic[key] += 1
            else:
                statistic[key] = 1
            # print("{} = {}".format(key, str(value)))
    except TypeError:
        print(error['typeerror'].format("add_entry"))


def write_statistic(file_path="stats.json"):
    # this functions seems rather redundant, but it makes main a bit prettier
    global statistic
    try:
        with open(file_path, 'w') as outfile:
            json.dump(statistic, outfile, indent=4)

        print("{} Bytes of Data were written to {}".format(os.stat(file_path).st_size, file_path))
    except FileExistsError:
        print("")
    except FileNotFoundError:
        print("")  # why should this ever happen?


def main():
    load_config()  # default path should suffice
    temp_json = load_remote_content(main_url, main_url_param)
    data = test_json(temp_json)
    if data:  # no else required, test_json already gives us an error if something fails
        traverse_json_data(data)
        write_statistic()



if __name__ == "__main__":
    main()
    print("Overall Executiontime was {} seconds".format(str(round(time.time() - start_time, 3))))
    print("Statistic has {} entries".format(len(statistic)))