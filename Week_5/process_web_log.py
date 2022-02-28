# import the libraries

from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

#defining DAG arguments

# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Kenny Van',
    'start_date': days_ago(0),
    'email': ['kvan@somemail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# define the DAG
dag = DAG(
    'process_web_log',
    default_args=default_args,
    description='DAG that processes a web log',
    schedule_interval=timedelta(days=1)
)

# define the tasks

# define the extraction task
extract_data = BashOperator(
    task_id = 'extract_data',
    bash_command = 'cut -d"-" -f1 accesslog.txt | tr -d "[:blank:]" > extracted_data.txt',
    dag = dag
)

# define the transformation task
transform_data = BashOperator(
    task_id = 'transform_data',
    bash_command = 'cat extracted_data.txt | sed "/198\.46\.149\.143/d" > transformed_data.txt',
    dag = dag
)

# define the load task
load_data = BashOperator(
    task_id = 'load_data',
    bash_command = 'tar xvf weblog.tar transformed_data.txt',
    dag = dag
)

# define the pipeline
extract_data >> transform_data >> load_data