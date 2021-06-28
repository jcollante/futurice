import json
import pandas as pd
import csv
from io import StringIO
from pyjstat import pyjstat


def clean(extracts: dict=dict()):
    """
    Coordinates the cleansing and transformations
    """
    cleansed_df = {}
    for indx, content in extracts.items():
        if is_json(content):
            cleansed_df[indx] = to_dataframe(clean_json(content), is_json=True)
        else:
            cleansed_df[indx] = to_dataframe(content, is_json=False)
        
    return cleansed_df
            

def is_json(content):
    """
    Check if data has json format
    """
    try:
        content = clean_json(content)
        json_object = json.loads(str(content))
    except ValueError as e:
        return False
    
    return True


def clean_json(content=''):
    """
    Clean data from jsons
    """
    content = str(content).replace("\'", "\"")
    content = str(content).replace("None", "\"None\"")
    return str(content)


def to_dataframe(content, is_json=False):
    """
    Convert the response to dataframe
    """
    df = None
    if is_json:
        dataset = pyjstat.Dataset.read(content)
        df = dataset.write('dataframe')
    else:
        s = csv.Sniffer()
        delimiter = s.sniff(content).delimiter
        df = pd.read_csv(StringIO(content), sep=delimiter)
        
    return df