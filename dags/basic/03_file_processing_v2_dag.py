from datetime import datetime, timedelta
import os
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.exceptions import AirflowFailException

from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.operators.bash import BashOperator

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

# ---------- Helpers ----------
def resolve_path(file_path: str, base_dir: str) -> str:
    """
    Resolve file_path relative to base_dir (if not absolute).
    Prevent path traversal: ensure final path stays under base_dir.
    """
    if not file_path:
        raise AirflowFailException("Thiếu 'file_path' trong dag_run.conf")

    base_dir_abs = os.path.abspath(base_dir or "/")
    if not os.path.isabs(file_path):
        joined = os.path.normpath(os.path.join(base_dir_abs, file_path))
    else:
        joined = os.path.normpath(file_path)

    final_abs = os.path.abspath(joined)

    # Chặn thoát khỏi BASE_DATA_DIR
    if not os.path.commonpath([base_dir_abs, final_abs]) == base_dir_abs:
        raise AirflowFailException(
            f"Đường dẫn không hợp lệ (thoát khỏi BASE_DATA_DIR): {final_abs}"
        )
    return final_abs

# ---------- Python callables ----------
def task_resolve_and_return_abs_path(**context):
    dag_run = context["dag_run"]
    conf_path = (dag_run.conf or {}).get("file_path", "")
    base_dir = Variable.get("BASE_DATA_DIR", default_var="/opt/airflow/data")
    abs_path = resolve_path(conf_path, base_dir)
    print(f"✅ Resolved absolute path: {abs_path}")
    return abs_path

def file_exists_callable(ti, **_):
    # Lấy path đã resolve từ XCom
    path = ti.xcom_pull(task_ids="resolve_path")
    exists = os.path.exists(path)
    print(f"⏳ Waiting for file: {path} | exists={exists}")
    return exists

def inspect_file(file_path: str, **_):
    if not os.path.exists(file_path):
        raise AirflowFailException(f"File không tồn tại: {file_path}")

    st = os.stat(file_path)
    info = {
        "path": file_path,
        "name": Path(file_path).name,
        "size_bytes": st.st_size,
        "modified_at": datetime.fromtimestamp(st.st_mtime).isoformat(),
        "created_at": datetime.fromtimestamp(st.st_ctime).isoformat(),
    }
    print("📄 File info:", info)
    return info

# ---------- DAG ----------
default_args = {
    "owner": "fsa",
    "depends_on_past": False,
    "email": ["you@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    "on_failure_callback": slack_task_fail_alert,  # gửi Slack khi task fail
}

with DAG(
    dag_id="file_processing_v2",
    description="Resolve path with BASE_DATA_DIR, wait for file via Sensor, Slack alert on failure",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule=None,  # trigger thủ công
    catchup=False,
    tags=["file", "processing", "sensor", "slack"],
) as dag:

    # 1) Resolve path an toàn từ dag_run.conf + BASE_DATA_DIR
    resolve = PythonOperator(
        task_id="resolve_path",
        python_callable=task_resolve_and_return_abs_path,
    )

    # 2) Chờ file xuất hiện (PythonSensor) - tránh chiếm worker bằng mode="reschedule"
    wait_for_file = PythonSensor(
        task_id="wait_for_file",
        python_callable=file_exists_callable,
        poke_interval=30,         # kiểm tra mỗi 30s
        timeout=60,          # tối đa chờ 1 phút
        mode="reschedule",        # trả slot cho worker khi chờ
        soft_fail=False,          # đặt True nếu muốn DAG continue khi hết hạn chờ
    )

    # 3) Liệt kê chi tiết (quote path để an toàn ký tự đặc biệt)
    list_file = BashOperator(
        task_id="list_file_details",
        bash_command='ls -la "{{ ti.xcom_pull(task_ids=\'resolve_path\') }}"',
    )

    # 4) Phân tích/ghi log thông tin
    analyze = PythonOperator(
        task_id="inspect_file",
        python_callable=inspect_file,
        op_kwargs={"file_path": '{{ ti.xcom_pull(task_ids="resolve_path") }}'},
    )

    resolve >> wait_for_file >> list_file >> analyze
