# global python packages
import requests
import json

# local files
import local_tools

# load config data

with open('settings.json') as json_file:
    data = json.load(json_file)
    error = data['errors']
    if local_tools.is_dictkey(data, "url"):
        main_url = data['url']
        main_url_param = data['url_para']

# directly requests from solr
try:
    resp = requests.get(main_url, params=main_url_param)
    print("Trying URL {}", resp.url)
    print("Request Code {}", resp)
    print("Content:")
    print(resp.text)
except requests.exceptions.RequestException as e:
    print(error['file'], e)