Shopify module for the [snapflow](https://github.com/kvh/snapflow) framework.

#### Install

`pip install snapflow-shopify` or `poetry add snapflow-shopify`

#### Example

```python
from snapflow import Graph, produce
from snapflow_shopify import module as shopify

g = graph()

# Initial graph
orders = g.create_node(
    "shopify.import_orders",
    params={"shopify_admin_url": "xxx:xxx@your-shop.myshopify.com"},
)
blocks = produce(orders, env=env, modules=[shopify])
print(blocks[0].as_records())
```
