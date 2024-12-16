# Tradesignals Platform

This project is under active development. Please be aware that there may be frequent updates and changes. We recommend regularly checking the repository for the latest updates and patches. When we are ready to drop an alpha release, we will update the README.md file and remove this warning. In the meantime, give us a star and follow our developers on GitHub!

This guide will help you understand how to configure and manage your Tradesignals Streaming Application project.

## Technologies

Tradesignals Streaming Application is built on top of the best, most performant, and scalable technologies.

### Cloud Services

Our software is hosted on `Amazon Web Services` in both the `us-east-1` and `us-west-2` regions.

### Programming Languages

The primary language used is `Python 3.11.X` for the `FastAPI` server and `Rust 1.79.0` for big data processing and machine learning computations.

### Databases

The `Clickhouse` database is used for real-time data storage and retrieval. `Clickhouse` can be up to 100x faster than `Postgres` and `MySQL` for read-intensive workloads.

### Messaging Queue

The `Apache Kafka` messaging queue is used for streaming data ingestion and distribution.

### Data Processing

The `Apache Spark` data processing framework is used for batch data processing and machine learning dataset preparation.

### Data Storage

The `Clickhouse` database is used for real-time data storage and retrieval. `Clickhouse` can be up to 100x faster than `Postgres` and `MySQL` for read-intensive workloads.

### Data Transfer

The `Apache Arrow` in-memory data format is used for data transfer between the `Clickhouse` database and the `Kafka` messaging queue. `Apache Arrow` is a columnar data format optimized for performance and scalability. The exported files are compressed using the `Apache Parquet` format on `Amazon S3`.

### Technology Dependencies

| Tech                             | Purpose                                | Version |
| -------------------------------- | -------------------------------------- | ------- |
| Clickhouse                       | Lightning Fast Database                | 24.12.0 |
| Python                           | API Language                           | 3.11    |
| Rust                             | Signal Processing and Machine Learning | 1.79.0  |
| FastAPI                          | HTTP API Framework                     | 4.1.0   |
| TensorFlow                       | Machine Learning Framework             | 2.15.0  |
| PyTorch                          | Machine Learning Framework             | 2.1.0   |
| Apache Spark                     | Data Processing Framework              | 3.5.0   |
| Apache Arrow                     | In-Memory Data Format                  | 20.0.0  |
| Apache Flink                     | Stream Processing                      | 2.1.0   |
| Apache Kafka                     | Messaging Queue                        | 3.5.0   |
| Apache Kafka Schema Registry     | Schema Management                      | 2.1.0   |
| Helm                             | Kubernetes Package Manager             | 3.12.0  |
| Kubernetes                       | Container Orchestration                | 1.30.0  |
| GitHub Actions                   | CI/CD                                  | 2.1.0   |
| Docker                           | Containerization                       | 25.0.0  |
| Docker Compose                   | Multi-Container Management             | 2.20.0  |
| Confluent Kafka                  | Messaging Queue                        | 3.5.0   |
| Amazon S3                        | Object Storage                         | Cloud   |
| Amazon Elastic Container Service | Container Orchestration                | Cloud   |
| Amazon Athena                    | Interactive Query Service              | Cloud   |
| Amazon CloudWatch                | Monitoring and Logging                 | Cloud   |
| Amazon CloudFront                | Content Delivery Network               | Cloud   |
| Amazon API Gateway               | API Management                         | Cloud   |
| Amazon SNS                       | Notification Service                   | Cloud   |
| Amazon SQS                       | Message Queue Service                  | Cloud   |
| Amazon Lambda                    | Serverless Computing                   | Cloud   |
| Amazon EventBridge               | Event Bus Service                      | Cloud   |

## Structure

Each pipeline is stored in its own separate directory. When a pipeline is deployed, each individual pipeline is deployed as a Docker container. Docker Compose is used to manage the deployment of the pipelines.

### Docker Compose

Refreshing the `docker-compose.yaml` file with the latest pipeline configurations can be done easily by running the `docker compose pull` command.

### Docker Manager

In the `./scripts` directory, there is a `docker_manager.py` command-line application that provides many useful utilities when working with multiple pipelines.

### Tradesignals CLI

The `tradesignals.sh` script is a wrapper around the `docker_manager.py` script and provides a command-line interface for managing the pipelines.

Enter `./scripts/tradesignals.sh --help` for more details on each of the CLI commands and properties.

## Streaming Data Pipelines

Streaming pipelines are defined in the `pipeline.yaml` configuration file. This file is used to configure the streaming application itself.

Pipelines have their own `pipeline.yaml` file that is used to configure the pipeline itself. Additionally, a global registry of topics, pipelines, and schemas is used to define the topics that are used by the pipeline.

### `pipelines.yaml` Global Pipeline Registry

The `pipelines.yaml` file is used to configure the streaming application. It is split into three sections:

1. `StreamingApplication`: The root of the streaming application.
2. `Topics`: A collection of topics used by the streaming application.
3. `Pipelines`: A collection of pipelines used by the streaming application.

Topics and pipelines are referenced by the `id` field in their respective sections.

For example, the following is a configuration file for a streaming application that processes option trades and aggregates them into volume bars.

### `topics.yaml`

Topic configurations are stored in a separate file called `topics.yaml`.

The `pipelines.yaml` file references the `topics.yaml` file using the `Topics` property.

### `topics.yaml` Configuration

The `topics.yaml` file is a list of dictionaries that contains the configuration for the topics that are used by the pipeline.

### Project Structure

Tradesignals Streaming Application projects are organized into a directory structure to efficiently manage the project.

Each pipeline is stored in its own separate directory. When a pipeline is deployed, each individual pipeline is deployed as a Docker container. Docker Compose is used to manage the deployment of the pipelines.

Refreshing the `docker-compose.yaml` file with the latest pipeline configurations can be done easily by running the `docker compose pull` command.

In the `./scripts` directory, there is a `docker_manager.py` command-line application that provides many useful utilities when working with multiple pipelines.

The `tradesignals.sh` script is a wrapper around the `docker_manager.py` script and provides a command-line interface for managing the pipelines.

Enter `./scripts/tradesignals.sh --help` for more details on each of the CLI commands and properties.

[!INFO] The name of the pipeline directory must match the pipeline ID in the `pipelines.yaml` file.

    tradesignals/
        ├── .env  # environment variables for the project (required)
        ├── docs/  # documentation for the project
        ├── tests/  # unit tests for the project
        ├── scripts/  # scripts for the project
        ├── requirements.txt  # dependencies for the project
        ├── docker-compose.yaml  # docker compose file for the project
        └── src/
            └── streaming/
                ├── pipelines.yaml  # pipeline configuration file
                ├── topics.yaml  # topic configuration files
                └── pipelines/
                    ├── pipeline_id_1/
                    │   ├── config.yaml
                    │   ├── app.py
                    │   ├── Dockerfile
                    │   ├── requirements.txt
                    │   ├── pyproject.toml
                    │   ├── README.md
                    │   ├── .env
                    │   ├── .gitignore
                    │   ├── .dockerignore
                    │   └── ...
                    ├── pipeline_id_2/
                    │   └── ...
                    └── ...

The `topics.yaml` file is located in the same directory as the `pipelines.yaml` file in the root of the pipelines package (`src/pipelines`).

```yaml
topics:
  - topic_id: option_trades
    reset_topic_on_start: true
  - topic_id: option_trades_aggs
    reset_topic_on_start: false
```

**YAML Property**: `[topics]` _(root of the topics.yaml file)_

**Model Class**: `tradesignals.streaming.types.StreamingPipelineTopics`

The `pipeline.topics` configuration is a list of dictionaries that contains the configuration for the topics that are used by the pipeline.

The following properties are supported:

| Property               | Description                               | Default Value |
| ---------------------- | ----------------------------------------- | ------------- |
| `topic_id`             | The id of the topic.                      | N/A           |
| `reset_topic_on_start` | Whether to delete and recreate the topic. | False         |

## `topic` Configurations

The Topic Configuration Dictionary is a dictionary that contains the configuration for a topic.

The following properties are supported:

| Property              | Description                                                                                 | Default Value |
| --------------------- | ------------------------------------------------------------------------------------------- | ------------- |
| `id`                  | The id of the topic.                                                                        | N/A           |
| `num_partitions`      | The number of partitions for the topic.                                                     | `4`           |
| `replication_factor`  | The replication factor for the topic.                                                       | `3`           |
| `min_insync_replicas` | The minimum number of in-sync replicas for the topic.                                       | `2`           |
| `retention_ms`        | The retention time for the topic in milliseconds. Defaults to millis equivalent of 30 days. | `2592000000`  |
| `retention_bytes`     | The retention size for the topic in bytes. Defaults to unlimited.                           | `-1`          |
| `cleanup_policy`      | Values include: `delete`, and `compact`                                                     | `delete`      |
| `key_serializer`      | Values include: `string`, `int`, `long`, `double`                                           | `string`      |
| `value_serializer`    | Values include: `avro`, `json`, `protobuf`                                                  | `avro`        |
| `value_schema_id`     | Reference to the id of a schema in the `Schemas` section.                                   | `None`        |

## Pipelines

Streaming pipelines are defined in the `pipeline.yaml` configuration file.  This file is used to be able to define pipeline infrastructure in ithe source code repository and create a layer of abstraction between the pipeline logic and the pipeline infrastructure.

The following is an example of a streaming pipeline configuration file.

### `pipeline.yaml`

The Pipeline Configuration Dictionary is a dictionary that contains the configuration for a pipeline.

### `pipeline`

model class: `_streaming.types.StreamingPipeline_`

#### `StreamingPipeline` Configuration Properties

| Property    | Description                     | Default Value |
| ----------- | ------------------------------- | ------------- |
| `id`        | The id of the pipeline.         | N/A           |
| `log_level` | The log level for the pipeline. | INFO          |
| `input`     | The input for the pipeline.     | N/A           |
| `output`    | The output for the pipeline.    | N/A           |

#### `pipeline.input`

The `pipeline.input` configuration is a dictionary that contains the configuration for the input of a pipeline.

The following properties are supported:

| Property              | Description                      | Default Value      |
| --------------------- | -------------------------------- | ------------------ |
| `type`                | The type of the input.           | `websocket`        |
| `kafka_topic_id`      | The id of the kafka topic.       | `None`             |
| `group_id`            | The name of the consumer group.  | `default-group-id` |
| `module_path`         | `PipelineInput`implementation    | `None`             |
| `class_name`          | The name of the class.           | `None`             |
| `additional_metadata` | Key/value pairs Tuple[str, str]. | `None`             |

## Pipeline Output Configuration

**YAML Property**: `pipeline.output`

**Model Class**: `tradesignals.streaming.types.StreamingPipelineOutput`

The Pipeline Output Dictionary is a dictionary that contains the configuration for the output of a pipeline.

### `output` properties

| Property              | Description                         | Default Value |
| --------------------- | ----------------------------------- | ------------- |
| `type`                | The type of the output.             | `kafka`       |
| `topic_id`            | The id of the kafka topic.          | `None`        |
| `publisher_id`        | The id of the data source.          | `None`        |
| `dataset_id`          | The id of the dataset.              | `None`        |
| `entitlements`        | Required entitlements for the data. | `[]`          |
| `additional_metadata` | Key/value pairs Tuple[str, str].    | `None`        |

## Docker Configuration

The `pipeline.docker` configuration is a dictionary that contains the configuration for the docker container that is used to run the pipeline.

The following properties are supported:

| Property | Description                        | Default Value |
| -------- | ---------------------------------- | ------------- |
| `image`  | The image to use for the pipeline. | N/A           |

## Example Configuration File

# StreamingApplication is the root of the streaming application

```yaml

    StreamingApplication:
    id: example_application
    environment: production # environment is the environment the application is running in.
    log_level: INFO # log_level is the log level for the application
    reset_topics_on_start: true # reset_topics_on_start is a flag to reset the topics on start

    # Topics is a collection of topics that are used by the streaming application.
    Topics:

        # option_trades is a topic that is used to store the option trades.
        option_trades:
        num_partitions: 4
        replication_factor: 3
        min_insync_replicas: 2
        retention_ms: 2592000000
        retention_bytes: -1
        cleanup_policy: delete
        key_serializer: string
        value_serializer: avro
        value_schema:
            namespace: option_trades
            name: OptionTrades
            type: record
            version: 1
            subject: option_trades-value
            doc: Option trades aggregated by symbol and timestamp

        # option_trades_aggs is a topic that is used to store the aggregated option trades.
        option_trades_aggs:
        num_partitions: 4
        replication_factor: 3
        min_insync_replicas: 2
        retention_ms: 2592000000
        retention_bytes: -1
        cleanup_policy: delete
        key_serializer: string
        value_serializer: avro
        value_schema:
            namespace: option_trades_aggs
            name: OptionTradesAggs
            type: record
            version: 1
            subject: option_trades_aggs-value
            doc: Option trades aggregated by symbol and timestamp
            fields:
            - name: symbol
            type: string
            doc: Symbol of the option
            min_length: 1
            max_length: 10
            - name: timestamp
            type: long
            logical_type: timestamp-millis
            doc: Timestamp of the trade
            - name: oi
            type: int
            doc: Open interest for the option contract at the time of the trade.
            - name: price
            type: bytes
            logical_type: decimal
            precision: 2
            scale: 2
            doc: Price of the option at the time of the trade.

    # Pipelines is a collection of pipelines that are used by the streaming application.
    Pipelines:

        # option_trade_volume_bars is a pipeline that is used to process the option trades.
        option_trade_volume_bars:
        id: OptionTradeVolumeBars
        log_level: INFO
        input:
            type: kafka
            consumer_group_id: option_volume_aggs_consumer
            kafka_topic_id: option_trades
        output:
            type: kafka
            kafka_topic_id: option_trades_aggs
```
