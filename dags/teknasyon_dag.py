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
    schedule_interval= None  #'@daily'
)

with dag:


    partition_table_bash = BashOperator(
        task_id='partition_table_bash',
        bash_command='cd /home/atakanozkan98/teknasyon_case; source venv/bin/activate; python create_partition_table.py linux',
        do_xcom_push=False
    )


    cs_to_bq_bash = BashOperator(
        task_id='cs_to_bq_bash',
        bash_command='cd /home/atakanozkan98/teknasyon_case; source venv/bin/activate; python cloud_storage_to_bq.py linux',
        do_xcom_push=False
    )

    clean_data_bash = BashOperator(
        task_id='clean_data_bash',
        bash_command='cd /home/atakanozkan98/teknasyon_case; source venv/bin/activate; python clean_data.py linux',
        do_xcom_push=False
    )

    adjust_data_bash = BashOperator(
        task_id='adjust_data_bash',
        bash_command='cd /home/atakanozkan98/teknasyon_case; source venv/bin/activate; python adjust_data.py linux',
        do_xcom_push=False
    )

    final_report_bash = BashOperator(
        task_id='final_report_bash',
        bash_command='cd /home/atakanozkan98/teknasyon_case; source venv/bin/activate; python final_report.py linux',
        do_xcom_push=False
    )

partition_table_bash >> cs_to_bq_bash >> clean_data_bash >> adjust_data_bash >> final_report_bash