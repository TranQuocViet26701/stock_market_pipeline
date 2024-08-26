import json
from airflow.hooks.base import BaseHook
import requests


def _get_stock_prices(url: str, symbol: str) -> str:
    api = BaseHook.get_connection("stock_api")
    resp = requests.get(
        url=f"{url}{symbol}",
        headers=api.extra_dejson["headers"],
        params={"metrics": "high", "interval": "1d", "range": "1y"},
    )
    return json.dumps(resp.json()["chart"]["result"][0])
