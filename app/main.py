import inspect
import requests 
import json
import base64
import io
import logging 
from static.pipeline_helper import PipelineHelper
from flask import Flask, render_template, request
from dlt_platform.connectors import *
from dlt_platform.transforms import *

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

app = Flask(__name__)
# Load environment variables from the .env file
with open('./config.env') as f:
    for line in f:
        key, value = line.strip().split('=')
        app.config[key] = value

env_vars = {
            'DB_PAT': app.config.get('AZURE_DB_PAT'), 'DB_URL': app.config.get('AZURE_DB_URL'),
            'AZURE_DB_PAT': app.config.get('AZURE_DB_PAT'), 'AZURE_DB_URL': app.config.get('AZURE_DB_URL'),
            'GCP_DB_PAT': app.config.get('GCP_DB_PAT'), 'GCP_DB_URL': app.config.get('GCP_DB_URL')
            }

ph = PipelineHelper(env_vars=env_vars, app=app)

@app.route('/')
def index():
    # need to conver this to a dictionary ? Something better probably a database ?
    select_options = [
        {'name': 'Select an Option', 'init_variables': [], 'function_options': [], 'function_parameters': []}
        , generate_lists(file_source_connect.FileSourceConnect)
        , generate_lists(delta_lake_connect.DeltaLakeConnect)
        , generate_lists(edw_connect.EDWConnect)
        , generate_lists(jdbc_connect.JDBCConnect)
        , generate_lists(kafka_connect.KafkaConnect)
        , generate_lists(sql_transform.SQLTransform)
        ]
    
    connector_box_html = generate_box_html('Data Connector', select_options)

    cloud_dropdown = ['Azure', 'GCP']

    return render_template('index.html', connector_box_html=connector_box_html, select_options=select_options, cloud_dropdown=cloud_dropdown)

def update_helper_vars():
    ph.env_vars = env_vars

@app.route('/create_pipeline', methods=['POST'])
def create_pipeline():
    data = request.json
    cloud = ph.env_vars.get('cloud').upper() if ph.env_vars.get('cloud')  is not None else 'AZURE'
    logging.info(f"--------------------> {cloud}")
    logging.info(data)
    ph.update_pipeline_json('app/static/create_pipeline.json'
                        , data.get('target_database')
                        , data.get('job_name')
                        , data.get('working_directory') )

    notebook_file = ph.create_notebook(data.get('boxes'))
    logging.info("-----> Notebook Updated. Starting Notebook Upload.")

    notebook_results = ph.upload_notebook(working_directory=data.get('working_directory')
                    , job_name=data.get('job_name')
                    , file_obj=notebook_file)
    logging.info(f"Notebook Uploaded: {notebook_results.status_code} | {notebook_results.content}")
    
    logging.info("Creating Pipeline.")
    pipeline_results = ph.upload_dlt_pipeline(file_path='app/static/create_pipeline.json')
    app.config['pipeline_id'] = json.loads(pipeline_results.content.decode()).get('pipeline_id') 
    app.config[f'{cloud}_pipeline_id'] = json.loads(pipeline_results.content.decode()).get('pipeline_id') 
    env_vars['pipeline_id'] = json.loads(pipeline_results.content.decode()).get('pipeline_id') 
    env_vars[f'{cloud}_pipeline_id'] = json.loads(pipeline_results.content.decode()).get('pipeline_id')     
    update_helper_vars()
    logging.info(f"Pipeline Upload Done. Starting Pipeline Now: {pipeline_results.status_code} | {pipeline_results.content} | {app.config.get('pipeline_id')}")
    ph.start_pipeline(app.config.get('pipeline_id'))
    logging.info("Pipeline Started")

    return '200'

@app.route('/update_pipeline', methods=['POST'])
def update_pipeline():
    data = request.json
    # Update Notebook Object
    notebook_file = ph.create_notebook(data.get('boxes'))
    logging.info("-----> Notebook Updated. Starting Notebook Upload.")

    # Send Notebook to Workspace
    notebook_results = ph.upload_notebook(working_directory=data.get('working_directory')
                    , job_name=data.get('job_name')
                    , file_obj=notebook_file)
    logging.info(f"Notebook Uploaded: {notebook_results.status_code} | {notebook_results.content}")

    return '200'

@app.route('/start_pipeline', methods=['POST'])
def start_pipeline():
    data = request.json

    logging.info(f"Starting Pipeline Now: {app.config.get('pipeline_id')}")
    ph.start_pipeline(env_vars.get('pipeline_id'), full_refresh=True)
    logging.info("Pipeline Started")

    return '200'

@app.route('/delete_pipeline', methods=['POST'])
def delete_pipeline():
    db_auth = {"Authorization": "Bearer {}".format(ph.env_vars.get('DB_PAT'))}

    results = requests.delete(url=f"{ph.env_vars.get('DB_URL')}api/2.0/{ph.env_vars.get('pipeline_id')}/pipelines", 
                            headers=db_auth
                            )

    return results

@app.route('/update_cloud', methods=['POST'])
def update_cloud():
    data = request.json 
    cloud = data.get('cloud').upper() if data.get('cloud')  is not None else 'AZURE'
    logging.info(f"{f'{cloud}_DB_URL'} | app.config.get(f'{cloud}_DB_URL') ")
    env_vars['cloud'] = cloud
    env_vars['DB_URL'] = app.config.get(f'{cloud}_DB_URL')
    env_vars['DB_PAT'] = app.config.get(f'{cloud}_DB_PAT')
    env_vars['pipeline_id'] = env_vars.get(f'{cloud}_pipeline_id')
    update_helper_vars()
    logging.info(f"Cloud {cloud} Update: {ph.env_vars} ")
    return '200'


def get_function_parameters(pyclass):
    """ Function to get parameters. See example return structure. 
        # [
        #     {
        #     'name': '', 
        #     'function_parameters': []
        #     },
        #     {
        #     'name': '', 
        #     'function_parameters': []
        #     }
        # ]
    """
    function_parameters = [{'name': 'Select a Function', 'function_parameters': []}]
    for i in [i for i in dir(pyclass) if '__' not in i]:
        # for each function in the class
        # i is the function name
        # params is the list of parameters for the function
        params = [p for p in list(inspect.signature(getattr(pyclass, i)).parameters.keys()) if p not in ('self')]
        d = {'name': i, 'function_parameters': params}
        function_parameters.append(d)
    return function_parameters


def generate_box_html(box_id, options):
    options_html = ""
    for option in options:
        options_html += f"<option value='{option}'>{option}</option>"
    
    box_html = "<div id="+box_id+"' class='box'><div class='box-header'>"+box_id+" </div> <div class='box-content'><select>"+options_html+"</select></div></div>"
    return box_html

def generate_lists(pyclass):
    function_options = ['Select a Function'] + [i for i in dir(pyclass) if '__' not in i]
    init_vars = list(inspect.signature(pyclass.__init__).parameters)
    init_vars.remove('self') if 'self' in init_vars else None
    init_vars.remove('args') if 'args' in init_vars else None
    init_vars.remove('kwargs') if 'kwargs' in init_vars else None

    func_parameters = get_function_parameters(pyclass)

    # I can reduce the amount of data I am storing with the function stuff
    return {
        'name': pyclass.__name__ ,
        'init_variables': init_vars,
        'function_options': function_options,
        'function_parameters': func_parameters
    }


if __name__ == '__main__':
    app.run(debug=True, use_reloader=False)



