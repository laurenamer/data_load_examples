"""The Default Pipeline Template provides a simple starting point for your dlt pipeline"""

# mypy: disable-error-code="no-untyped-def,arg-type"

import dlt
from dlt.common import Decimal
from dlt.sources.rest_api import RESTAPIConfig, rest_api_source


@dlt.resource(name="customers", primary_key="id")
def census_customers():
    """Load customer data from a simple python list."""
    yield [
        {"id": 1, "name": "simon", "city": "berlin"},
        {"id": 2, "name": "violet", "city": "london"},
        {"id": 3, "name": "tammo", "city": "new york"},
    ]


@dlt.resource(name="inventory", primary_key="id")
def census_inventory():
    """Load inventory data from a simple python list."""
    yield [
        {"id": 1, "name": "apple", "price": Decimal("1.50")},
        {"id": 2, "name": "banana", "price": Decimal("1.70")},
        {"id": 3, "name": "pear", "price": Decimal("2.50")},
    ]


@dlt.source(name="my_fruitshop")
def census_source():
    """A source function groups all resources into one schema."""
    return census_customers(), census_inventory()


def load_stuff() -> None:
    # specify the pipeline name, destination and dataset name when configuring pipeline,
    # otherwise the defaults will be used that are derived from the current script name
    p = dlt.pipeline(
        pipeline_name='census',
        destination='bigquery',
        dataset_name='census_data',
    )

    census_source = rest_api_source({
        "client": {
            "base_url": "https://api.census.gov/data/2020/dec/sdhc",
            "auth": {
                "token": dlt.secrets["census_key"],
            },
        },
        "resource_defaults": {
            "endpoint": {
                "params": {
                    "limit": 1000,
                },
            },
        },
        "resources": [
            "names",
        ],
        }
    )

    load_info = p.run(census_source())

    # pretty print the information on data that was loaded
    print(load_info)  # noqa: T201


if __name__ == "__main__":
    load_stuff()
