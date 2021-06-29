import os
import json 
from flask_api import FlaskAPI
from flask_api import status
from flask import request
from dotenv import load_dotenv
import sqlite3

# ETL tasks
from etl.extract import extract
from etl.cleanse import clean
from etl.load import load
from etl.transformations import transform, get_list_country_attractiveness

db_name = 'futurice'
app = FlaskAPI(__name__)

# Load env variables
load_dotenv()
url_list = json.loads(os.getenv('ENV_DATASOURCES', list()))
db_name = os.getenv('ENV_DB_NAME', "default_db")

app.config["DEBUG"] = True

@app.route('/get_results', methods=['GET'])
def get_results():
    """ 
    Return results
    """
    result = get_list_country_attractiveness(db_name)

    if not result:
        return "Record not found", status.HTTP_422_UNPROCESSABLE_ENTITY
    return result

@app.route('/update_data', methods=['PUT'])
def update_data(): # using query parameters
    """ 
    Runs the etl: get data from source, clean, transform and upload data
    """
    
    # ETL Tasks
    extracts = extract(url_list)
    cleanse = clean(extracts)
    loads = load(cleanse, db_name)
    transform(cleanse, db_name)
    
    if loads != 200:
        return "Record not found", status.HTTP_422_UNPROCESSABLE_ENTITY
    else:
        return {
            'update_data': "success"
        }

app.run()