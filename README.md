Shopify module for the [snapflow](https://github.com/kvh/snapflow) framework.

#### Install

`pip install snapflow-shopify` or `poetry add snapflow-shopify`

#### Example

```python
from snapflow import Graph, produce
import snapflow_shopify as shopify

g = graph()

# Initial graph
orders = g.create_node(
    "shopify.extract_orders",
    params={"shopify_admin_url": "xxx:xxx@your-shop.myshopify.com"},
)
output = produce(orders, env=env, modules=[shopify])
print(output.as_records())
```
