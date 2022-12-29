
# tap-shipstation

[![License: AGPLv3](https://img.shields.io/badge/License-AGPLv3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)
[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

This tap:

- Pulls raw data from ShipStation's API (https://www.shipstation.com/developer-api/)
- Extracts the following resources:
  - [Orders](https://www.shipstation.com/developer-api/#/reference/model-order)
  - [Shipments](https://www.shipstation.com/developer-api/#/reference/shipments/list-shipments/list-shipments-w/o-parameters)
  - [Stores](https://www.shipstation.com/docs/api/stores/list/)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

### Getting started

Create a config JSON file with the following format. The `default_start_datetime` should be in Pacific Time (required by the ShipStation API, see [here](https://www.shipstation.com/developer-api/#/introduction/shipstation-api-requirements/datetime-format-and-time-zone)).

```
{
  "api_key": "Your ShipStation API Key",
  "api_secret": "Your ShipStation API Secret",
  "default_start_datetime": "2018-11-01 00:00:00"
}
```

You can obtain your ShipStation API key by following the instructions [here](https://help.shipstation.com/hc/en-us/articles/206638917-How-can-I-get-access-to-ShipStation-s-API-).

---

Copyright &copy; 2018 Milk Bar
