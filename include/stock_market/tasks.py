import json
from io import BytesIO

from minio import Minio
import requests
from airflow.hooks.base import BaseHook


def _get_stock_prices(url: str, symbol: str) -> str:
    api = BaseHook.get_connection("stock_api")
    resp = requests.get(
        url=f"{url}{symbol}",
        headers=api.extra_dejson["headers"],
        params={"metrics": "high", "interval": "1d", "range": "1y"},
    )
    return json.dumps(resp.json()["chart"]["result"][0])


def _store_prices(stock: str):
    minio = BaseHook.get_connection("minio")
    client = Minio(
        minio.extra_dejson["endpoint"].split("//")[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False,
    )

    bucket_name = "stock-market"
    found = client.bucket_exists(bucket_name)
    if not found:
        client.make_bucket(bucket_name)
        print("Created bucket ", bucket_name)
    else:
        print("Bucket ", bucket_name, " already exists")

    stock = json.loads(stock)
    symbol = stock["meta"]["symbol"]
    data = json.dumps(stock, ensure_ascii=False).encode("utf-8")
    client.put_object(
        bucket_name=bucket_name,
        object_name=f"{symbol}/prices.json",
        data=BytesIO(data),
        length=len(data),
    )
