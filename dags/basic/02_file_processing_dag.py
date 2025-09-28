from datetime import datetime, timedelta
from site import abs_paths
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.exceptions import AirflowFailException
import os

# Slack
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

# ---------- Slack failure callback ----------
def slack_task_fail_alert(context):
    ti = context.get("task_instance")
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown_dag"
    task_id = ti.task_id if ti else "unknown_task"
    run_id = context.get("run_id", "unknown_run")
    log_url = ti.log_url if ti else ""
    exec_date = context.get("ts", "")

    msg = (
        ":red_circle: *Task Failed*\n"
        f"*DAG*: `{dag_id}`\n"
        f"*Task*: `{task_id}`\n"
        f"*Run*: `{run_id}`\n"
        f"*When*: `{exec_date}`\n"
        f"*Log*: {log_url}"
    )
    SlackWebhookOperator(
        task_id="notify_slack_failure",
        http_conn_id="slack_webhook",
        message=msg,
    ).execute(context=context)

def check_file_exists(file_path=None, base_path=None, **context):
    """
    - Nhận file_path từ op_kwargs (được template từ dag_run.conf)
    - Nếu path tương đối => join với BASE_DATA_DIR
    - Kiểm tra tồn tại; return đường dẫn tuyệt đối để downstream dùng qua XCom
    """
    if not file_path:
        raise AirflowFailException("Thiếu 'file_path' trong dag_run.conf")

    if not os.path.isabs(file_path):
        file_path = os.path.join(base_path, file_path)

    abs_path = os.path.abspath(file_path)
    if not os.path.exists(abs_path):
        raise AirflowFailException(f"File không tồn tại: {abs_path}")

    print(f"✅ File exists: {abs_path}")
    return abs_path

default_args = {
    'owner': 'fsa', # người sở hữu DAG
    'depends_on_past': False, # không phụ thuộc vào lần chạy trước
    'start_date': datetime(2025, 1, 1), # ngày bắt đầu chạy DAG
    'email': ['congdinh2021@gmail.com'], # email nhận thông báo
    'email_on_failure': False, # không gửi email khi thất bại
    'email_on_retry': False, # không gửi email khi thử lại
    'retries': 0, # số lần thử lại khi thất bại
    'retry_delay': timedelta(minutes=0.5), # thời gian chờ giữa các lần thử lại
    'on_failure_callback': slack_task_fail_alert,  # gửi Slack khi task fail
    'on_retry_callback': slack_task_fail_alert,    # gửi Slack khi task retry

}

with DAG(
    dag_id="file_processing",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["file", "processing"],
) as dag:

    check_file = PythonOperator(
        task_id="check_file_exists",
        python_callable=check_file_exists,
        op_kwargs={
            'file_path': '{{ dag_run.conf.get("file_path", "Users.csv") }}',
            'base_path': '{{ var.value.BASE_DATA_DIR | default("/opt/airflow/data/meta/raw") }}',
        },
    )

    list_file = BashOperator(
        task_id="list_file",
        bash_command="ls -la {{ ti.xcom_pull(task_ids='check_file_exists') }}",
    )

    check_file >> list_file

