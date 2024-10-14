from airflow import DAG
from datetime import datetime, timedelta, date
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable


DB_CONN = "gp_std7_59"
DB_SCHEMA = 'std7_59'
F_LOAD_DM = 'f_load_mart_fin'
F_DELTA_PARTITION = "f_load_delta_partition"
F_LOAD_FULL = 'load_full_ch'

LOAD_TABELS_LF = ["stores", "coupons", "promos", "promo_types"]
LOAD_FILES_LF = {"stores": "stores2", "coupons": "coupons5", "promos": "promos2", "promo_types": "promo_types2"}

LOAD_TABELS_DP = ['bills_head', 'bills_item', 'traffic']
LOAD_PART_DP = {'bills_head': 'calday', 'bills_item': 'calday', 'traffic': 'date'}

DELTA_PART_QUERY = f"select {DB_SCHEMA}.{F_DELTA_PARTITION}(%(table_name)s, %(part_value)s, '2021-01-01', '2021-02-28', '', '');"
FULL_LOAD_QUERY = f"select {DB_SCHEMA}.{F_LOAD_FULL}(%(tab_name)s, %(file_name)s);"
LOAD_DM_QUERY = f"select {DB_SCHEMA}.{F_LOAD_DM}('2021-01-01', '2021-02-28')"


default_args = {
    'depends_on_past': False,
    'owner': 'std7_59',
    'start_date': datetime(2024, 9, 23),
    'retrise': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    "std7_59_fin_proj",
    max_active_runs=1,
    schedule_interval=None,
    default_args=default_args,
    catchup=False
) as dag:

    task_start = DummyOperator(task_id="start")
    
    
    with TaskGroup("insert_dp") as task_insert_dp:
         for table in LOAD_TABELS_DP:
            task = PostgresOperator(task_id=f"load_table_{table}",
                                    postgres_conn_id=DB_CONN,
                                    sql=DELTA_PART_QUERY,
                                    parameters={'table_name': f'{DB_SCHEMA}.{table}', 'part_value': f'{LOAD_PART_DP[table]}'}
                                   )
            
    with TaskGroup("full_insert_load") as task_full_load:
        for table in LOAD_TABELS_LF:
            task = PostgresOperator(task_id=f"load_table_{table}",
                                    postgres_conn_id=DB_CONN,
                                    sql=FULL_LOAD_QUERY,
                                    parameters={'tab_name': f'{DB_SCHEMA}.{table}', 'file_name': f'{LOAD_FILES_LF[table]}'}
                                   )
            
    task_load_dm = PostgresOperator(task_id="insert_dm",
                                 postgres_conn_id=DB_CONN,
                                 sql=LOAD_DM_QUERY
                                 )
            
    task_end = DummyOperator(task_id="end")
            
    task_start >> task_insert_dp >> task_full_load >> task_load_dm >> task_end