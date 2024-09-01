import json
from io import BytesIO

from minio import Minio
import requests
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowNotFoundException

BUCKET_NAME = "stock-market"


def _get_stock_prices(url: str, symbol: str) -> str:
    api = BaseHook.get_connection("stock_api")
    resp = requests.get(
        url=f"{url}{symbol}",
        headers=api.extra_dejson["headers"],
        params={"metrics": "high", "interval": "1d", "range": "1y"},
    )
    return json.dumps(resp.json()["chart"]["result"][0])


def _store_prices(stock: str) -> str:
    client = _get_minio_connection()
    found = client.bucket_exists(BUCKET_NAME)
    if not found:
        client.make_bucket(BUCKET_NAME)
        print("Created bucket ", BUCKET_NAME)
    else:
        print("Bucket ", BUCKET_NAME, " already exists")

    stock = json.loads(stock)
    symbol = stock["meta"]["symbol"]
    data = json.dumps(stock, ensure_ascii=False).encode("utf-8")
    client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=f"{symbol}/prices.json",
        data=BytesIO(data),
        length=len(data),
    )

    return f"{BUCKET_NAME}/{symbol}"


def _get_minio_connection():
    minio = BaseHook.get_connection("minio")
    client = Minio(
        minio.extra_dejson["endpoint_url"].split("//")[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False,
    )

    return client


def _get_formatted_csv(path: str):
    client = _get_minio_connection()

    objects = client.list_objects(
        bucket_name=BUCKET_NAME,
        prefix=f"{path.split('/')[1]}/formatted_prices/",
        recursive=True,
    )

    for obj in objects:
        if obj.object_name.endswith(".csv"):
            return obj.object_name
    return AirflowNotFoundException("The CSV file does not exist")
