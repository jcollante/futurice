import os
import json
import requests
from pyjstat import pyjstat


def extract(url_dict: dict = dict()):
    """
    Get data from datasources
    """
    response_dict = {}
    for indx, url in url_dict.items():
        r = requests.get(url)
        if r.status_code == 200:
            try:
                response = pyjstat.request(url)
                response_dict[indx] = response
            except Exception as e:
                response_dict[indx] = r.text
                
    return response_dict
