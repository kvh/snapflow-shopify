from typing import TypeVar
from snapflow import SnapflowModule

from .snaps.import_orders import import_orders


ShopifyOrder = TypeVar("ShopifyOrder")

module = SnapflowModule(
    "shopify",
    py_module_path=__file__,
    py_module_name=__name__,
    schemas=["schemas/shopify_order.yml"],
    snaps=[import_orders],
)
module.export()
