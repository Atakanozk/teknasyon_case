from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
import os

DAG_ID = 'teknasyon_dag'

args = {
    'owner' : 'Atakan Özkan',
    'start_date' : days_ago(1)
}

dag = DAG(
    dag_id=DAG_ID,
    default_args=args,
    schedule_interval='@daily'
)

with dag:

    test_dummy = DummyOperator(
        task_id='test_dummy'
    )

    partition_table_bash = BashOperator(
        task_id='partition_table_bash',
        bash_command='cd /home/atakanozkan98/teknasyon_case; source venv/bin/activate; python create_partition_table.py linux',
        do_xcom_push=False
    )

    final_report_bash = BashOperator(
        task_id='final_report_bash',
        bash_command='cd /home/atakanozkan98/teknasyon_case; source venv/bin/activate; python final_report.py linux',
        do_xcom_push=False
    )

test_dummy >> partition_table_bash >> final_report_bash