# climate-ref-example

[![PyPI version](https://badge.fury.io/py/climate-ref-example.svg)](https://badge.fury.io/py/climate-ref-example)
[![Documentation Status](https://readthedocs.org/projects/climate-ref/badge/?version=latest)](https://climate-ref.readthedocs.io/en/latest/?badge=latest)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python Version](https://img.shields.io/badge/python-3.9%2B-blue)](https://www.python.org/downloads/)

This package provides an example implementation of a metrics provider for the Climate REF (Rapid Evaluation Framework).
It serves as a template and reference for developers who want to create their own metrics providers.

## Installation

```bash
pip install climate-ref-example
```

## Features

- Example implementation of a basic metrics provider
- Simple counter metric demonstration
- Complete implementation of all required interfaces
- Documentation and comments explaining the implementation

## Usage

```bash
# Run the example metrics through REF
ref run-metrics --provider example --input /path/to/data
```

## Example Metric

The package implements a simple counter metric that demonstrates the basic structure of a REF metric:

```python
from climate_ref_core import BaseProvider, Metric

class ExampleProvider(BaseProvider):
    def get_metrics(self):
        return [ExampleMetric()]

class ExampleMetric(Metric):
    def calculate(self, data):
        return len(data)
```

## Documentation

For detailed documentation, please visit [https://climate-ref.readthedocs.io/](https://climate-ref.readthedocs.io/)

## Contributing

Contributions are welcome! Please see the main project's [Contributing Guide](https://climate-ref.readthedocs.io/en/latest/contributing/) for more information.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
