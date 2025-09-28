from datetime import datetime, timedelta
from email.policy import default
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def print_hello():
    print("Hello World from Airflow")
    return 'success'

default_args = {
    'owner': 'fsa', # người sở hữu DAG
    'depends_on_past': False, # không phụ thuộc vào lần chạy trước
    'start_date': datetime(2025, 1, 1), # ngày bắt đầu chạy DAG
    'email': ['congdinh2021@gmail.com'], # email nhận thông báo
    'email_on_failure': False, # không gửi email khi thất bại
    'email_on_retry': False, # không gửi email khi thử lại
    'retries': 1, # số lần thử lại khi thất bại
    'retry_delay': timedelta(minutes=0.5), # thời gian chờ giữa các lần thử lại
}

# Define the DAG chạy 1p một lần
dag = DAG(
    dag_id="hello_world",
    default_args=default_args,
    schedule="*/1 * * * *",
    catchup=False,
    tags=["check"],
)

task1 = BashOperator(
    task_id="print_date",
    bash_command="date",
    dag=dag,
)

task2 = PythonOperator(
    task_id="print_hello",
    python_callable=print_hello,
    dag=dag,
)

# Define task dependencies
task1 >> task2
