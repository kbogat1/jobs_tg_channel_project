from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from extraction_jobs_api_to_s3 import extract_hh_prof_roles

import pendulum

OWNER = "k.bogatyrev"
DAG_ID = "from_head_hunter_api_to_pg_professional_roles"
STORE = 'ODS'
SOURCE = 'head_hunter'

LOGIN = Variable.get("POSTGRES_LOGIN")
PASSWORD = Variable.get("POSTGRES_PASSWORD")

default_args = {
    'owner': OWNER,
    'start_date': pendulum.datetime(day=15, month=9, year=2025),
    'retries': 0,
    'tags': [SOURCE, STORE],
    'catchup': False
}

with DAG(
    dag_id=DAG_ID,
    description="Extracting raw data of professional roles from head hunter api to postgre database",
    default_args=default_args
):
    
    start = EmptyOperator(
        task_id="start"
    )
    
    get_data_head_hunter_professional_roles = PythonOperator(
        task_id = "get_data_head_hunter_professional_roles",
        python_callable=extract_hh_prof_roles,
        op_kwargs={"user_name":LOGIN,
                   "password":PASSWORD}
    )
    
    end = EmptyOperator(
        task_id="end"
    )
    
    start >> get_data_head_hunter_professional_roles >> end