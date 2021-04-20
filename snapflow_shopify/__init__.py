from typing import TypeVar

from snapflow import SnapflowModule

from .functions.import_orders import import_orders

ShopifyOrder = TypeVar("ShopifyOrder")

module = SnapflowModule(
    "shopify",
    py_module_path=__file__,
    py_module_name=__name__,
)
module.add_function(import_orders)
