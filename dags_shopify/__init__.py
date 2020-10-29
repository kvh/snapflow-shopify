from dags.core.module import DagsModule

from .pipes.extract_shopify_orders import extract_shopify_orders


module = DagsModule(
    "shopify",
    py_module_path=__file__,
    py_module_name=__name__,
    schemas=["schemas/shopify_order.yml"],
    pipes=[extract_shopify_orders],
)
module.export()
