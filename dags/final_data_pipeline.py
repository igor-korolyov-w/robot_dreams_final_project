from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from tasks.tasks import load_to_bronze_from_pg, load_to_silver_from_pg, load_to_dwh_from_pg, load_to_bronze_from_api, load_to_silver_from_api, load_to_dwh_from_api, getTableFromPg

table_tasks_pg_to_bronze = []
table_tasks_pg_to_silver = []
table_tasks_pg_to_dwh = []

dag_pg = DAG(
    dag_id="a_pg_data_pipeline9",
    description="pg data pipeline",
    start_date=datetime(2021, 11, 14, 14, 30),
    schedule_interval='@daily'
)

dag_api = DAG(
    dag_id="api_data_pipeline9",
    description="api data pipeline",
    start_date=datetime(2021, 8, 30, 14, 30),
    end_date=datetime(2021, 9, 2, 14, 30),
    schedule_interval='@daily'
)

dummy1 = DummyOperator(task_id="start_load_to_bronze", dag=dag_pg)
dummy2 = DummyOperator(task_id="end_load_to_bronze", dag=dag_pg)
dummy3 = DummyOperator(task_id="start_load_to_silver", dag=dag_pg)
dummy4 = DummyOperator(task_id="end_load_to_silver", dag=dag_pg)
dummy5 = DummyOperator(task_id="start_load_to_dwh", dag=dag_pg)
dummy6 = DummyOperator(task_id="end_load_to_dwh", dag=dag_pg)

for table in getTableFromPg():
    table_tasks_pg_to_bronze.append(
        PythonOperator(
        task_id=f"table_to_bronze_{table}",
        python_callable=load_to_bronze_from_pg,
        provide_context=True,
        dag=dag_pg,
        op_kwargs={'table': table}
        )
    )

for table in getTableFromPg():
    table_tasks_pg_to_silver.append(
        PythonOperator(
        task_id=f"table_to_silver_{table}",
        python_callable=load_to_silver_from_pg,
        provide_context=True,
        dag=dag_pg,
        op_kwargs={'table': table}
        )
    )

for table in getTableFromPg():
    table_tasks_pg_to_dwh.append(
        PythonOperator(
        task_id=f"table_to_dwh_{table}",
        python_callable=load_to_dwh_from_pg,
        provide_context=True,
        dag=dag_pg,
        op_kwargs={'table': table}
        )
    )

t1 = PythonOperator(
    task_id='load_to_bronze_from_api',
    provide_context=True,
    python_callable=load_to_bronze_from_api,
    dag=dag_api)

t2 = PythonOperator(
    task_id='load_to_silver_from_api',
    provide_context=True,
    python_callable=load_to_silver_from_api,
    dag=dag_api)

t3 = PythonOperator(
    task_id='load_to_dwh_from_api',
    provide_context=True,
    python_callable=load_to_dwh_from_api,
    dag=dag_api)

dummy1 >> table_tasks_pg_to_bronze >> dummy2 >> dummy3 >> table_tasks_pg_to_silver >> dummy4 >> dummy5 >> table_tasks_pg_to_dwh >> dummy6
t1 >> t2 >> t3

















