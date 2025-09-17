from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from extraction_jobs_api_to_s3 import get_data_hh_prof_roles

import pendulum

OWNER = "k.bogatyrev"
DAG_ID = "from_head_hunter_api_to_s3_professional_roles"
STORE = 'raw'
SOURCE = 'head_hunter'

ACCESS_KEY = Variable.get("S3_ACCESS_KEY")
SECRET_KEY = Variable.get("S3_SECRET_KEY")

default_args = {
    'owner': OWNER,
    'start_date': pendulum.datetime(day=15, month=9, year=2025),
    'retries': 0,
    'tags': [SOURCE, STORE],
    'catchup': False
}

with DAG(
    dag_id=DAG_ID,
    description="Extracting raw data of professional roles from head hunter api to s3",
    default_args=default_args
):
    
    start = EmptyOperator(
        task_id="start"
    )
    
    get_data_head_hunter_professional_roles = PythonOperator(
        task_id = "get_data_head_hunter_professional_roles",
        python_callable=get_data_hh_prof_roles,
        op_kwargs={"ACCESS_KEY":ACCESS_KEY,
                   "SECRET_KEY":SECRET_KEY}
    )
    
    end = EmptyOperator(
        task_id="end"
    )
    
    start >> get_data_head_hunter_professional_roles >> end