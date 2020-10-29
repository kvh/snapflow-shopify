from __future__ import annotations

import base64
from dataclasses import dataclass
from typing import Dict, Tuple

from dags import DataSet, DataBlock
from dags.core.data_formats import RecordsList, RecordsListGenerator
from dags.core.extraction.connection import JsonHttpApiConnection
from dags.core.pipe import pipe
from dags.core.runnable import PipeContext


DEFAULT_MIN_DATE = "2006-01-01 00:00:00"  # Before Shopify was founded
API_VERSION = "2020-01"


def split_admin_url(admin_url: str) -> Tuple[str, str, str]:
    auth, url = admin_url.split("@")
    shop_name = url.split(".")[0]
    return (url, auth, shop_name)


def url_and_headers_from_admin_url(admin_url: str) -> Tuple[str, Dict]:
    url, auth, shop_name = split_admin_url(admin_url)
    token = base64.b64encode(auth.encode()).decode("ascii")
    headers = {"Authorization": f"Basic {token}"}
    shop_url = f"https://{shop_name}.myshopify.com/admin/api/{API_VERSION}"
    return shop_url, headers


def get_next_page_link(resp_headers):
    if not resp_headers:
        return None
    values = resp_headers.get("Link", resp_headers.get("link"))
    if not values:
        return None
    result = {}
    for value in values.split(", "):
        link, rel = value.split("; ")
        result[rel.split('"')[1]] = link[1:-1]
    return result.get("next")


@dataclass
class ExtractShopifyOrdersConfig:
    shopify_admin_url: str


@dataclass
class ExtractShopifyOrdersState:
    latest_updated_at: str


@pipe(
    "extract_orders",
    module="shopify",
    config_class=ExtractShopifyOrdersConfig,
    state_class=ExtractShopifyOrdersState,
)
def extract_shopify_orders(
    ctx: PipeContext,
) -> RecordsListGenerator[shopify.ShopifyOrder]:
    admin_url = ctx.get_config_value("shopify_admin_url")
    _, _, shop_name = split_admin_url(admin_url)
    url, headers = url_and_headers_from_admin_url(admin_url)
    endpoint_url = url + "/orders.json"
    latest_updated_at = ctx.get_state_value("latest_updated_at") or DEFAULT_MIN_DATE

    params = {
        "order": "updated_at asc",
        "updated_at_min": latest_updated_at,
        "status": "any",
        "limit": 250,
    }
    conn = JsonHttpApiConnection()
    while True:
        resp = conn.get(endpoint_url, params, headers=headers)
        json_resp = resp.json()
        assert isinstance(json_resp, dict)
        records = json_resp["orders"]
        if len(records) == 0:
            # All done
            break
        new_latest_updated_at = max([o["updated_at"] for o in records])
        ctx.emit_state({"latest_updated_at": new_latest_updated_at})
        yield records
        # Shopify has cursor-based pagination now, so we can safely paginate results
        next_page = get_next_page_link(resp.headers)
        if not next_page:
            # No more pages
            break
        params = {"page_info": next_page, "limit": 250}
