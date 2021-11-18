import airflow
import json
import pathlib
import pprint
import requests
import re
from datetime import date
import pandas as pd
import requests.exceptions as requests_exceptions
from airflow import DAG
#from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import  PythonOperator


dag = DAG(
    dag_id="JobFlow",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval=None
)


def _get_jobs():
    today = date.today()
    d1 = today.strftime("%d/%m/%Y")
    d1 = d1.replace('/', '')
    result = (requests.get(
        'https://www.themuse.com/api/public/jobs?category=Data%20Science&category=Software%20Engineer&page=10')).json()
    with open("/tmp/"+str(d1)+".json", 'w') as outfile:
        json.dump(result, outfile)

get_jobs= PythonOperator(
task_id="get_jobs",
python_callable=_get_jobs,
dag=dag,
)

def _process_jobs():
    # Lets check if can get the results
    today = date.today()
    d1 = today.strftime("%d/%m/%Y")
    d1 = d1.replace('/', '')
    jobs = []
    with open("/tmp/"+str(d1)+".json",'r') as f:
        result = json.load(f)
        for x in range(len(result["results"])):
            cleantext = re.sub(re.compile('<.*?>'), '', result["results"][x]["contents"])
            jobs.append([cleantext,result["results"][x]["name"],
                        result["results"][x]["locations"][0]["name"],
                        result["results"][x]["categories"][0]["name"],
                        result["results"][x]["levels"][0]["name"],
                        result["results"][x]["refs"]["landing_page"],
                        result["results"][x]["company"]["name"]])

    df = pd.DataFrame(jobs, columns=['Content', 'Job', 'Location', 'Category', 'Level', 'URL', 'Company'])
    df.to_csv("/tmp/"+str(d1)+".csv", index=True)

process_jobs= PythonOperator(
task_id="process_jobs",
python_callable=_process_jobs,
dag=dag,
)


def _notify_jobs():
    print("Today's job is ready :)")

notify_jobs= PythonOperator(
task_id="notify_jobs",
python_callable=_notify_jobs,
dag=dag,
)


get_jobs >> process_jobs >> notify_jobs
