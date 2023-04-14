import inspect
import requests 
import json
import base64
import io
import logging 
from flask import Flask, render_template, request
from dlt_platform.connectors import *
from dlt_platform.transforms import *

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

class PipelineHelper():
    def __init__(self, env_vars, app):
        self.env_vars = env_vars
        self.app = app

    def start_pipeline(self, pipeline_id, full_refresh=False):
        """ Starts a DLT pipeline 
        Source: https://docs.databricks.com/delta-live-tables/api-guide.html#start-a-pipeline-update
        """
        db_auth = {"Authorization": "Bearer {}".format(self.env_vars.get('DB_PAT'))}
        data = {'full_refresh': full_refresh}

        logging.info(f"{self.env_vars.get('DB_URL')}api/2.0/pipelines/{pipeline_id}/updates ||| {db_auth}")


        results = requests.post(url=f"{self.env_vars.get('DB_URL')}api/2.0/pipelines/{pipeline_id}/updates", 
                                headers=db_auth
                                # , data = data
                                )
        return results


    def create_notebook(self, boxes):
        file = io.StringIO()
        file.write(
"""
# Databricks notebook source
# MAGIC %pip install git+https://github.com/rchynoweth/StreamingTemplates.git@main

# COMMAND ----------
import dlt 
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Custom Python Library - i.e. "template"
from dlt_platform.connectors.kafka_connect import KafkaConnect
from dlt_platform.connectors.file_source_connect import FileSourceConnect
from dlt_platform.connectors.jdbc_connect import JDBCConnect
from dlt_platform.connectors.delta_lake_connect import DeltaLakeConnect
from dlt_platform.transforms.sql_transform import SQLTransform

# COMMAND ----------
""" )
        for i, b in enumerate(boxes):
            dataset_name = b.get('init_variables')[0]
            ## add init variables
            init_vars = ','.join(f"'{e}'" for e in b.get('init_variables')[1:])
            file.write(f"v{i} = {b.get('class')}({init_vars}) \n")
            ## spark context

            function_vars = ','.join(f"'{e}'" for e in b.get('function_parameters') if e != '')
            function_vars = function_vars.replace("'spark'", "spark")

            # rtn_str = ""
            # if b.get('class') != "SQLTransform":
            #     rtn_str = f"\treturn ( v{i}.{b.get('function')}({function_vars}) )\n"
            # else :
            #     rtn_str = f"\treturn spark.sql({function_vars})"
            rtn_str = f"\treturn ( v{i}.{b.get('function')}({function_vars}) )\n"

            file.write(f"@dlt.table(name='{dataset_name}') \n")
            file.write(f"def {dataset_name}(): \n")
            file.write(rtn_str)
            file.write(f"\n# COMMAND ----------\n\n")
        
        return file

    def upload_notebook(self, working_directory, job_name, file_obj):
        """ Uploads a notebook to Databricks 
        Source: https://docs.databricks.com/dev-tools/api/latest/workspace.html#import
        """

        db_auth = {"Authorization": "Bearer {}".format(self.env_vars.get('DB_PAT'))}

        file_base64 = base64.b64encode(file_obj.getvalue().encode()).decode()
        results = requests.post(url=f"{self.env_vars.get('DB_URL')}api/2.0/workspace/import", 
                                headers=db_auth,
                                json={
                                    'path': f"{working_directory}{job_name}",
                                    'content': file_base64,
                                    'language': 'PYTHON',
                                    'overwrite': True,
                                    'format': 'SOURCE'
                                    }
                                )
        return results

    def upload_dlt_pipeline(self, file_path):
        db_auth = {"Authorization": "Bearer {}".format(self.env_vars.get('DB_PAT'))}

        with open(file_path, 'rb') as f:
            file_data = f.read()

        results = requests.post(url=f"{self.env_vars.get('DB_URL')}api/2.0/pipelines", 
                                headers=db_auth,
                                data = file_data
                                )

        return results



    def update_pipeline_json(self, file_path, target_database, job_name, working_dir):
        with open(file_path, 'r') as file:
            data = json.load(file)

        data['name'] = job_name
        data['target'] = target_database
        data['libraries'][0]['notebook']['path'] = f"{working_dir}{job_name}"

        with open(file_path, 'w') as file:
            json.dump(data, file)

