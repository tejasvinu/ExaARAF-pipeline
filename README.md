# ExaARAF Pipeline

A complete data pipeline using Docker, Prometheus, Kafka, Spark, and InfluxDB.

## Architecture

1. **Prometheus** scrapes metrics from 4 endpoints (IPMI, Node, DCGM, and SLURM exporters)
2. **Prometheus Kafka Adapter** forwards metrics to Kafka topics
3. **Kafka** streams data to be consumed by Telegraf and Spark
4. **Telegraf** processes and forwards metrics to InfluxDB
5. **InfluxDB** stores the time-series data for visualization and analysis
6. **Spark** (optional) provides additional processing capabilities

## Setup Instructions

1. Ensure Docker and Docker Compose are installed on your system
2. Clone this repository
3. Run the setup script:

```bash
chmod +x setup.sh
./setup.sh
```

## Monitoring the Pipeline

- Prometheus UI: http://localhost:9099
- InfluxDB UI: http://localhost:8086
- Spark Master UI: http://localhost:8082
- Kafka: Accessible on localhost:9092 (for local applications)
- Additional Kafka brokers: localhost:9097, localhost:9095

## Accessing the Data

You can query InfluxDB directly through its web interface at http://localhost:8086, or use the InfluxDB API with the following credentials:

- Organization: metrics_org
- Bucket: metrics_bucket
- Username: admin
- Password: adminpassword123
- Token: my-super-secret-auth-token

Example InfluxDB query using the Flux query language:
```flux
from(bucket: "metrics_bucket")
  |> range(start: -1h)
  |> filter(fn: (r) => r["_measurement"] == "ipmi_metrics")
  |> yield()
```

## Component Details

- **Prometheus**: Configured to scrape metrics from the specified endpoints
- **Kafka**: Set up with four topics (ipmi_metrics, node_metrics, dcgm_metrics, slurm_metrics)
- **Telegraf**: Processes Kafka messages and writes to InfluxDB
- **InfluxDB**: Stores time-series metrics with high performance and compression
- **Spark**: (Optional) Provides additional data processing capabilities

## Customization

Edit the corresponding configuration files to customize:
- Prometheus scrape targets: `prometheus/prometheus.yml`
- Telegraf configuration: `telegraf/telegraf.conf`
- Spark processing logic: `spark/apps/process_metrics.py`
