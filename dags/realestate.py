from airflow.decorators import dag, task_group, task
from airflow.operators.python import PythonOperator
from pendulum import datetime, duration
from airflow.exceptions import AirflowFailException

# from include.realestate import _crawl_and_download_zip
# from include.realestate import _extract_zip_file
# from include.realestate import _store_prices
from minio import Minio
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowNotFoundException
import os

import json

BUCKET_NAME = "realestate-market"
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.exceptions import AirflowNotFoundException

from airflow.models import Variable


def _get_years_seasons_from_executation_date(logical_date=None):
    roc_year = int(logical_date.year) - 1911
    season = (int(logical_date.month) - 1) // 3 + 1
    # to get previous season
    if season == 1:
        season = 4
        roc_year -= 1
    else:
        season -= 1
    # print(f"here is the roc_year {roc_year}  season  {season}")
    return roc_year, season


def _crawl_and_download_zip(ti=None, logical_date=None):
    import requests

    # import os
    year, season = _get_years_seasons_from_executation_date(logical_date)
    # executation_date=ti.xcom_pull(key='executation_date',task_ids='get_years_seasons_from_executation_date')
    # year=executation_date['roc_year']
    # season=executation_date['season']

    REAL_ESTATE_URL = f"https://plvr.land.moi.gov.tw//DownloadSeason?season={year}S{season}&type=zip&fileName=lvr_landcsv.zip"

    response = requests.get(REAL_ESTATE_URL)
    if response.status_code != 200:
        print("request failed")
        raise AirflowFailException
    else:
        zip_file_path = f"/tmp/{year}{season}.zip"
        # path=f"/usr/local/airflow/include/{year}{season}.zip"
        # path='user/local/airflow/include/'+str(year)+str(season)+".zip"
        with open(zip_file_path, "wb") as f:
            f.write(response.content)
        output_data = {"response_path": zip_file_path, "year": year, "season": season}
        ti.xcom_push(key="zip_file_info", value=output_data)
        # ti.xcom_push(key='response_path',value=zip_file_path)
        # print("here is the path",zip_file_path)
        # return path


def _extract_zip_file(ti=None):
    import os
    import zipfile

    zip_file_info = ti.xcom_pull(
        key="zip_file_info", task_ids="crawl_and_download_zip"
    )  # 得到完整字典
    path = zip_file_info["response_path"]
    year = zip_file_info["year"]
    season = zip_file_info["season"]
    # print(f"here is response_path {path} here is the year{year} season {season}")
    # folder=f"{path}/realEstate{year}{season}"

    folder = f"real_estate{year}{season}"
    # file_name=f"{year}{season}.zip"

    # make additional folder for files to extract
    # if not os.path.isdir(folder):
    #     os.mkdir(folder)

    os.makedirs(folder, exist_ok=True)

    # extract files to the folder
    with zipfile.ZipFile(path, "r") as zip_ref:
        # with zipfile.ZipFile(file_name,"r") as zip_ref:
        zip_ref.extractall(folder)

    csv_file_path = os.path.join(folder, "a_lvr_land_a.csv")
    # print(f"here is the csv file path {csv_file_path}")

    # push unzipped file path
    # print(f"folder: {folder}  path : {path}")
    # ti.xcom_push(key="csv_file_path", value=csv_file_path)
    return csv_file_path
    # ti.xcom_push(key='folder',value=folder)


def _get_minio_client():
    minio = BaseHook.get_connection("minio")
    client = Minio(
        endpoint=minio.extra_dejson["endpoint_url"].split("//")[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False,
    )
    return client


# def _store_prices():   only for testing minio connection
def _store_prices(file_path):
    client = _get_minio_client()
    # Make the bucket if it doesn't exist.
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
        print(f"Created bucket{BUCKET_NAME}")
    else:
        print(f"Bucket {BUCKET_NAME} already exists")

    # The file to upload, change this path if needed
    source_file = file_path
    #  source_file = "/tmp/test-file.txt"

    destination_file = file_path
    # print(f"here is the source_file  {source_file}  here is the destination_file {destination_file}")
    #     # Upload the file, renaming it in the process
    client.fput_object(
        bucket_name=BUCKET_NAME,
        object_name=destination_file,
        file_path=source_file,
    )
    print(
        f"{source_file} successfully uploaded as object{destination_file} to bucket {BUCKET_NAME}"
    )

    #     here is the source_file  real_estate1144/a_lvr_land_a.csv
    #  here is the destination_file real_estate1144/a_lvr_land_a.csv
    # return source_file
    return f"realestate-market/{destination_file}"


@dag(
    start_date=datetime(2026, 3, 1),
    catchup=False,
    # 搭配  astro@02e3d99c019f:/usr/local/airflow$ airflow dags backfill -s 2025-06-01 -e 2026-01-31 realEstate  catchup=True
    tags=["real_estate"],
    schedule="0 4 2 * *",
    # schedule='@daily',
    default_args={
        "retries": 1,
        "retry_delay": duration(seconds=5),
    },
    dagrun_timeout=duration(minutes=20),
    max_consecutive_failed_dag_runs=2,
    max_active_runs=1,
)
def realEstate():

    get_years_seasons_from_executation_date = PythonOperator(
        task_id="get_years_seasons_from_executation_date",
        python_callable=_get_years_seasons_from_executation_date,
    )

    crawl_and_download_zip = PythonOperator(
        task_id="crawl_and_download_zip",
        python_callable=_crawl_and_download_zip,
        # op_kwargs={"year":114,"season":1}
        # add this line of code
    )

    extract_zip_file = PythonOperator(
        task_id="extract_zip_file", python_callable=_extract_zip_file
    )

    store_prices = PythonOperator(
        task_id="store_prices",
        python_callable=_store_prices,
        # 這邊是file_path 不是從上一個程式而來 而是自己取名  並丟到_store_prices()
        op_kwargs={"file_path": '{{ti.xcom_pull(task_ids="extract_zip_file")}}'},
        # task_ids 後面是字串
        # op_kwargs={'stock': '{{ ti.xcom_pull(task_ids="get_stock_prices") }}'}
    )

    format_prices = DockerOperator(
        task_id="format_prices",
        # image='airflow/stock-app',
        image="airflow/realestate-app",
        container_name="format_prices",
        api_version="auto",
        auto_remove="force",
        # auto_remove="success",
        docker_url="tcp://docker-proxy:2375",
        network_mode="container:spark-master",
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        environment={
            "AWS_ACCESS_KEY_ID": Variable.get("AWS_ACCESS_KEY_ID"),
            "AWS_SECRET_ACCESS_KEY": Variable.get("AWS_SECRET_ACCESS_KEY"),
            "ENDPOINT": Variable.get("ENDPOINT"),
            "SPARK_APPLICATION_ARGS": '{{ ti.xcom_pull(task_ids="store_prices") }}',
        },
    )

    # crawl_and_download_zip >> extract_zip_file >> store_prices >> read_minio_csv

    (
        get_years_seasons_from_executation_date
        >> crawl_and_download_zip
        >> extract_zip_file
        >> store_prices
        >> format_prices
    )

    # crawl_and_download_zip(114,1)


realEstate()
