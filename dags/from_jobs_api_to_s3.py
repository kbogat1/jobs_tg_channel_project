from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from extraction_jobs_api_to_s3 import get_data_jobify

import pendulum

OWNER = "k.bogatyrev"
DAG_ID = "from_jobs_api_to_s3"
STORE = 'raw'
SOURCE = 'jobicy.com'

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
    description="Extracting raw data from jobicy api to s3",
    default_args=default_args
):
    
    start = EmptyOperator(
        task_id="start"
    )
    
    get_data_data_science = PythonOperator(
        task_id="get_data_data_science",
        python_callable=get_data_jobify,
        op_kwargs={"industry":"data-science",
                   "ACCESS_KEY":ACCESS_KEY,
                   "SECRET_KEY":SECRET_KEY}
    )
    
    get_data_engineering = PythonOperator(
        task_id="get_data_engineering",
        python_callable=get_data_jobify,
        op_kwargs={"industry":"engineering",
                   "ACCESS_KEY":ACCESS_KEY,
                   "SECRET_KEY":SECRET_KEY}
    )
    
    get_data_admin = PythonOperator(
        task_id="get_data_admin",
        python_callable=get_data_jobify,
        op_kwargs={"industry":"admin",
                   "ACCESS_KEY":ACCESS_KEY,
                   "SECRET_KEY":SECRET_KEY}
    )
    
    end = EmptyOperator(
        task_id="end"
    )

    start >> [get_data_data_science, get_data_engineering, get_data_admin] >> end


