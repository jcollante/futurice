import os
import json 
from flask_api import FlaskAPI
from flask_api import status
from flask import request
from dotenv import load_dotenv

# ETL tasks
from etl.extract import extract
from etl.cleanse import clean
from etl.load import load
from etl.transformations import transform

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
    conn = sqlite3.connect(f"{db_name}.db")
    c = conn.cursor()
    c.execute(
        """SELECT time, country, (countries_gdp * ict_gdp * comp_services_usage) AS attractivenes
           FROM {db_schema_prod}.fact_attractiveness_office;
        """
    )

    content = c.fetchall()
    c.close()

    data = [{'time': time, 'country': country, 'attractivenes': attractivenes} \
            for time, country, attractivenes in content]

    return {
        'request_data': data
    }

@app.route('/update_data', methods=['GET'])
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
        return "Record not found", status.HTTP_400_BAD_REQUEST
    else:
        return {
            'update_data': "success"
        }

app.run()