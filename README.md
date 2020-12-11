Shopify module for the [snapflow](https://github.com/kvh/snapflow) framework.

#### Install

`pip install snapflow-shopify` or `poetry add snapflow-shopify`

#### Example

```python
from snapflow import Graph, produce
import snapflow_fred

g = graph()

# Initial graph
gdp = g.create_node(
    "fred.extract_observations",
    config={"api_key": "xxxxx", "series_id": "gdp"},
)
output = produce(gdp, env=env, modules=[fred])
print(output.as_records_list())
```
