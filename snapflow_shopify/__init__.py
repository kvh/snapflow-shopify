from typing import TypeVar
from snapflow import SnapflowModule

from .pipes.extract_orders import extract_orders


ShopifyOrder = TypeVar("ShopifyOrder")

module = SnapflowModule(
    "shopify",
    py_module_path=__file__,
    py_module_name=__name__,
    schemas=["schemas/shopify_order.yml"],
    pipes=[extract_orders],
)
module.export()
