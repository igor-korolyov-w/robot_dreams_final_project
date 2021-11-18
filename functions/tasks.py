from datetime import datetime, date
from airflow import DAG
import requests
import json
import os
import psycopg2
from contextlib import closing
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from hdfs import InsecureClient
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import *
import logging
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, LongType, DateType
from pyspark.sql.utils import AnalysisException

def getTableFromPg():
    f = open(os.path.join('/', 'home', 'user', 'airflow', 'dags', 'sqls', 'dshop.sql'), "r")
    sql = f.read()

    hook = PostgresHook(postgres_conn_id='pg')
    with closing(hook.get_conn()) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            for row in cursor:
                table_list = row[0]

    t_list = table_list.split(',')
    return t_list


def load_to_bronze_from_pg(table, **kwargs):
    logging.info(f"Start loading table '{table}' to Bronze")

    try:
        ds = kwargs.get('ds', str(date.today()))

        client = InsecureClient(f'http://127.0.0.1:50070', user=Variable.get("user_hadoop_secret"))
        client.makedirs(os.path.join('/', 'datalake', 'bronze', 'dshop', table))

        pg_conn = BaseHook.get_connection('pg')
        pg_url = f"jdbc:postgresql://{pg_conn.host}:{pg_conn.port}/{pg_conn.schema}"
        pg_creds = {"user": pg_conn.login, "password": pg_conn.password}

        datem = datetime.strptime(ds, "%Y-%m-%d")
        client.makedirs(os.path.join('/', 'datalake', 'bronze', 'dshop', table, str(datem.year)))
        client.makedirs(os.path.join('/', 'datalake', 'bronze', 'dshop', table, str(datem.year), str(datem.year) + '_' + str(datem.month)))
        client.makedirs(os.path.join('/', 'datalake', 'bronze', 'dshop', table, str(datem.year), str(datem.year) + '_' + str(datem.month),
                       str(datem.year) + '_' + str(datem.month) + '_' + str(datem.day))
                     )

        spark = SparkSession.builder \
            .config('spark.driver.extraClassPath', '/home/user/postgresql-42.3.1.jar') \
            .master('local') \
            .appName("load_to_bronze_from_pg") \
            .getOrCreate()

        table_df = spark.read.jdbc(pg_url, table=table, properties=pg_creds)

        table_df.write.csv(os.path.join('/', 'datalake', 'bronze', 'dshop', table, str(datem.year), str(datem.year) + '_' + str(datem.month),
                                        str(datem.year) + '_' + str(datem.month) + '_' + str(datem.day)),
                           mode = 'overwrite', header = 'true'
                           )

    except Exception as ex:
        logging.info(ex)

    logging.info(f"End loading table '{table}' to Bronze. Load '{table_df.count()}' records")

def load_to_silver_from_pg(table, **kwargs):
    logging.info(f"Start loading table '{table}' to Silver")

    ds = kwargs.get('ds', str(date.today()))

    client = InsecureClient(f'http://127.0.0.1:50070', user=Variable.get("user_hadoop_secret"))
    client.makedirs(os.path.join('/', 'datalake', 'silver', 'dshop'))

    datem = datetime.strptime(ds, "%Y-%m-%d")

    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath', '/home/user/postgresql-42.3.1.jar') \
        .master('local') \
        .appName("load_to_silver_from_pg") \
        .getOrCreate()

    try:

        table_df = spark.read.option('header', True).option('inferSchema', True).csv(os.path.join('/', 'datalake', 'bronze', 'dshop', table, str(datem.year),
                               str(datem.year) + '_' + str(datem.month),
                               str(datem.year) + '_' + str(datem.month) + '_' + str(datem.day)))

        if table == "aisles":
            new_aisle = [[1001, 'new_aisle1'], [1002, 'new_aisle2'], [1003, 'new_aisle3']]
            new_aisle_df = spark.createDataFrame(new_aisle)
            table_df = table_df.union(new_aisle_df)

        elif table == "clients":
            table_df = table_df.drop('location_area_id')

        elif table == "departments":
            new_department = [[101, 'new_department1'], [102, 'new_department2'], [103, 'new_department3']]
            new_department_df = spark.createDataFrame(new_department)
            table_df = table_df.union(new_department_df)

        elif table == "orders":
            table_df = table_df.filter(table_df.order_date >= '2021-02-01')
            table_df.na.drop()

        elif table == "products":
            table_df = table_df.withColumn("department_id", F.when(table_df.department_id == 1, 101).otherwise(table_df.department_id))
            table_df = table_df.withColumn("department_id", F.when(table_df.department_id == 2, 102).otherwise(table_df.department_id))
            table_df = table_df.withColumn("department_id", F.when(table_df.department_id == 3, 103).otherwise(table_df.department_id))

        table_df.write.parquet(os.path.join(os.path.join('/', 'datalake', 'silver', 'dshop', table)), mode = 'overwrite')

    except AnalysisException as e:
        logging.info(e)

    logging.info(f"End loading table '{table}' to Silver. Load '{table_df.count()}' records")

def load_to_dwh_from_pg(table, **kwargs):
    logging.info(f"Start loading table '{table}' to DWH")

    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath', '/home/user/postgresql-42.3.1.jar') \
        .master('local') \
        .appName("load_to_dwh_from_pg") \
        .getOrCreate()

    gp_conn = BaseHook.get_connection('gp')
    gp_url = f"jdbc:postgresql://{gp_conn.host}:{gp_conn.port}/{gp_conn.schema}"
    gpcreds = {"user": gp_conn.login, "password": gp_conn.password}
    mode = "overwrite"

    try:
        df_api = spark.read.parquet(os.path.join(os.path.join('/', 'datalake', 'silver', 'dshop', table)))

        if table == "orders":
            pg_table = "fact_orders"
            df_api = df_api.drop('store_id')

        else:
            pg_table = 'dim_'+table

        df_api.write.jdbc(url=gp_url, table=pg_table, mode=mode, properties=gpcreds)

        logging.info(f"Load '{df_api.count()}' records")

    except AnalysisException as e:
        logging.info(e)

    logging.info(f"End loading table '{table}' to DWH")



def load_to_bronze_from_api(**kwargs):
    logging.info("Start loading api_data to Bronze")

    ds = kwargs.get('ds', str(date.today()))

    client = InsecureClient(f'http://127.0.0.1:50070', user=Variable.get("user_hadoop_secret"))
    client.makedirs(os.path.join('/', 'datalake', 'bronze', 'api'))

    try:

        # получение токена из апи
        headers = {'content-type': 'application/json'}
        data = {"username": Variable.get("username_api_key"), "password": Variable.get("password_api_key")}
        response = requests.post(Variable.get("url_auth_api_key"), data=json.dumps(data), headers=headers, timeout=10)
        access_token = "JWT " + response.json()['access_token']

        headers = {'content-type': 'application/json', 'Authorization': access_token}
        data = {"date": ds}
        response = requests.get(Variable.get("url_data_api_key"), data=json.dumps(data), headers=headers, timeout=10)

        datem = datetime.strptime(ds, "%Y-%m-%d")
        client.makedirs(os.path.join('/', 'datalake', 'bronze', 'api', str(datem.year)))
        client.makedirs(os.path.join('/', 'datalake', 'bronze', 'api', str(datem.year), str(datem.year) + '_' + str(datem.month)))
        client.makedirs(os.path.join('/', 'datalake', 'bronze', 'api', str(datem.year), str(datem.year) + '_' + str(datem.month),
                        str(datem.year) + '_' + str(datem.month) + '_' + str(datem.day))
                        )

        if "message" in response.json():

            logging.info("!!! No data in API")

            with client.write(os.path.join('/', 'datalake', 'bronze', 'api', str(datem.year), str(datem.year) + '_' + str(datem.month),
                                      str(datem.year) + '_' + str(datem.month) + '_' + str(datem.day),
                                    'Error_'+ds + '.json'), encoding='utf-8', overwrite=True) as json_file_in_hdfs:
                json.dump(response.json(), json_file_in_hdfs)

        else:

            with client.write(os.path.join('/', 'datalake', 'bronze', 'api', str(datem.year),
                                          str(datem.year) + '_' + str(datem.month),
                                           str(datem.year) + '_' + str(datem.month) + '_' + str(datem.day),
                                           ds + '.json'), encoding='utf-8', overwrite=True) as json_file_in_hdfs:
                json.dump(response.json(), json_file_in_hdfs)

    except Exception as ex:
        logging.info(ex)

    logging.info("End loading api_data to Bronze")



def load_to_silver_from_api(**kwargs):
    logging.info("Start loading api_data to Silver")

    ds = kwargs.get('ds', str(date.today()))

    client = InsecureClient(f'http://127.0.0.1:50070', user=Variable.get("user_hadoop_secret"))
    client.makedirs(os.path.join('/', 'datalake', 'silver', 'api'))

    datem = datetime.strptime(ds, "%Y-%m-%d")

    spark = SparkSession.builder.master('local').appName("load_to_silver_from_api").getOrCreate()

    custom_api_schema = StructType([
            StructField("date", DateType(), False),
            StructField("product_id", LongType(), False)
        ])

    try:
        df_api = spark.read.schema(custom_api_schema) \
            .json(
            os.path.join('/', 'datalake', 'bronze', 'api', str(datem.year), str(datem.year) + '_' + str(datem.month),
                         str(datem.year) + '_' + str(datem.month) + '_' + str(datem.day),
                         ds + '.json'))

        df_api = df_api.dropDuplicates()

        df_api.na.drop()

        df_api.write.parquet(os.path.join(os.path.join('/', 'datalake', 'silver', 'api')), mode = 'append')
        logging.info(f"Load '{df_api.count()}' records")

    except AnalysisException as e:
        logging.info(e)

    logging.info("End loading api_data to Silver")

def load_to_dwh_from_api(**kwargs):
    logging.info("Start loading api_data to DWH")

    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath', '/home/user/postgresql-42.3.1.jar') \
        .master('local') \
        .appName("load_to_dwh_from_api") \
        .getOrCreate()

    gp_conn = BaseHook.get_connection('gp')
    gp_url = f"jdbc:postgresql://{gp_conn.host}:{gp_conn.port}/{gp_conn.schema}"
    properties = {"user": gp_conn.login, "password": gp_conn.password}
    mode = "overwrite"

    try:
        df_api = spark.read.parquet(os.path.join(os.path.join('/', 'datalake', 'silver', 'api')))
        df_api = df_api.select(F.col("date").alias("date_oos"), F.col("product_id").alias("product_id"))

        df_api.write.jdbc(url=gp_url, table="fact_oos", mode=mode, properties=properties)

        logging.info(f"Load '{df_api.count()}' records")

    except AnalysisException as e:
        logging.info(e)

    logging.info("End loading api_data to DWH")


