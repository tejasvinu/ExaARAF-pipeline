# ExaARAF Pipeline

A complete data pipeline using Docker, Prometheus, Kafka, Spark, and Cassandra.

## Architecture

1. **Prometheus** scrapes metrics from 4 endpoints (IPMI, Node, DCGM, and SLURM exporters)
2. **Prometheus Kafka Adapter** forwards metrics to Kafka topics
3. **Kafka** streams data to be consumed by Spark
4. **Spark** processes the metrics data
5. **Cassandra** stores the processed data for future use

## Setup Instructions

1. Ensure Docker and Docker Compose are installed on your system
2. Clone this repository
3. Run the setup script:

```bash
chmod +x setup.sh
./setup.sh
```

## Monitoring the Pipeline

- Prometheus UI: http://localhost:9091
- Spark Master UI: http://localhost:8082
- Kafka: Accessible on localhost:29092 (for local applications)

## Accessing the Data

You can query Cassandra using:

```bash
docker exec -it exaaraf-pipeline-cassandra-1 cqlsh
```

Then run:

```sql
USE metrics;
SELECT * FROM ipmi_metrics LIMIT 10;
SELECT * FROM node_metrics LIMIT 10;
SELECT * FROM dcgm_metrics LIMIT 10;
SELECT * FROM slurm_metrics LIMIT 10;
```

## Component Details

- **Prometheus**: Configured to scrape metrics from the specified endpoints
- **Kafka**: Set up with four topics (ipmi_metrics, node_metrics, dcgm_metrics, slurm_metrics)
- **Spark**: Processes data in 5-minute windows, calculating average, max, and min values
- **Cassandra**: Stores processed metrics in tables organized by metric type

## Customization

Edit the corresponding configuration files to customize:
- Prometheus scrape targets: `prometheus/prometheus.yml`
- Spark processing logic: `spark/apps/process_metrics.py`
- Cassandra schema: `cassandra/init.cql`
