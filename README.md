# Tradesignals Streaming

[![PyPI](https://img.shields.io/pypi/v/tradesignals-streaming.svg)][pypi_]
[![Status](https://img.shields.io/pypi/status/tradesignals-streaming.svg)][status]
[![Python Version](https://img.shields.io/pypi/pyversions/tradesignals-streaming)][python version]
[![License](https://img.shields.io/pypi/l/tradesignals-streaming)][license]

[![Read the documentation at https://tradesignals-streaming.readthedocs.io/](https://img.shields.io/readthedocs/tradesignals-streaming/latest.svg?label=Read%20the%20Docs)][read the docs]
[![Tests](https://github.com/macanderson/tradesignals-streaming/workflows/Tests/badge.svg)][tests]
[![Codecov](https://codecov.io/gh/macanderson/tradesignals-streaming/branch/main/graph/badge.svg)][codecov]

[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)][pre-commit]
[![Black](https://img.shields.io/badge/code%20style-black-000000.svg)][black]

[pypi_]: https://pypi.org/project/tradesignals-streaming/
[status]: https://pypi.org/project/tradesignals-streaming/
[python version]: https://pypi.org/project/tradesignals-streaming
[read the docs]: https://tradesignals-streaming.readthedocs.io/
[tests]: https://github.com/macanderson/tradesignals-streaming/actions?workflow=Tests
[codecov]: https://app.codecov.io/gh/macanderson/tradesignals-streaming
[pre-commit]: https://github.com/pre-commit/pre-commit
[black]: https://github.com/psf/black

## Features

Create containerized data pipeline services using a cli utility `ts-cli` run `ts-cli pipeline create` and follow the prompts.  Once completed, the system will have a template service configured and `docker-compose.yml` will be automatically adjusted to include the new service.

## Requirements

- `Python v3.11.10`
- `Apache-Flink (latest)`
- `Confluent` Cloud or Self-hosted Environment
- `Schema Registry`

## Installation

You can install _Tradesignals Streaming_ via [pip] from [PyPI]:

```zsh
pip install tradesignals-streaming
```

## Usage

Please see the [Command-line Reference] for details.

## Contributing

Contributions are very welcome.
To learn more, see the [Contributor Guide].

## License

Distributed under the terms of the [MIT license][license],
_Tradesignals Streaming_ is free and open source software.

## Issues

If you encounter any problems,
please [file an issue] along with a detailed description.

[license]: https://github.com/macanderson/tradesignals-streaming/blob/main/LICENSE
[contributor guide]: https://github.com/macanderson/tradesignals-streaming/blob/*main*/CONTRIBUTING.md
[command-line reference]: https://tradesignals-streaming.readthedocs.io/en/latest/usage.html
