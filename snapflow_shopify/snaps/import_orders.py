from __future__ import annotations

import base64
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, Iterator, Tuple

from dcp.data_format import Records
from requests.auth import HTTPBasicAuth
from snapflow import Param, Snap, SnapContext
from snapflow.core.extraction.connection import JsonHttpApiConnection

if TYPE_CHECKING:
    from snapflow_shopify import ShopifyOrder


DEFAULT_MIN_DATE = "2006-01-01 00:00:00"  # Before Shopify was founded
API_VERSION = "2020-01"


def split_admin_url(admin_url: str) -> Tuple[str, str, str]:
    auth, url = admin_url.split("@")
    shop_name = url.split(".")[0]
    return (url, auth, shop_name)


def url_and_auth_from_admin_url(admin_url: str) -> Tuple[str, Dict]:
    url, auth, shop_name = split_admin_url(admin_url)
    auth = HTTPBasicAuth(*auth.split(":"))
    shop_url = f"https://{shop_name}.myshopify.com/admin/api/{API_VERSION}"
    return shop_url, auth


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
class ImportShopifyOrdersState:
    latest_updated_at: str


@Snap(
    "import_orders",
    module="shopify",
    state_class=ImportShopifyOrdersState,
    display_name="Import Shopify orders",
)
@Param("shopify_admin_url", "str")
def import_orders(
    ctx: SnapContext,
) -> Iterator[Records[ShopifyOrder]]:
    admin_url = ctx.get_param("shopify_admin_url")
    _, _, shop_name = split_admin_url(admin_url)
    url, auth = url_and_auth_from_admin_url(admin_url)
    endpoint_url = url + "/orders.json"
    latest_updated_at = ctx.get_state_value("latest_updated_at") or DEFAULT_MIN_DATE

    params = {
        "order": "updated_at asc",
        "updated_at_min": latest_updated_at,
        "status": "any",
        "limit": 250,
    }
    conn = JsonHttpApiConnection()
    while ctx.should_continue():
        resp = conn.get(endpoint_url, params, auth=auth)
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
