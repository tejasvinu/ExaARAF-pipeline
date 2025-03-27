# ExaARAF Metrics Pipeline

A simplified metrics collection pipeline for ExaARAF that collects metrics from various exporters, processes them with Telegraf, and stores them in InfluxDB.

## Architecture

The pipeline follows a simple and efficient architecture:

```
Exporters → Prometheus → Telegraf → InfluxDB
```

- **Exporters**: IPMI, Node, DCGM, and Slurm exporters collect system metrics
- **Prometheus**: Scrapes metrics from exporters and stores them temporarily
- **Telegraf**: Queries metrics from Prometheus and processes them
- **InfluxDB**: Time-series database for long-term metrics storage and visualization

## Setup Instructions

1. Make sure Docker and Docker Compose are installed
2. Clone this repository
3. Run the setup script:
   ```bash
   ./setup.sh
   ```
4. Verify that the pipeline is working:
   ```bash
   ./check_pipeline.sh
   ```

## Accessing Dashboards

- InfluxDB UI: http://localhost:8086
  - Username: admin
  - Password: adminpassword123
  - Organization: metrics_org
  - Bucket: metrics_bucket
- Prometheus UI: http://localhost:9099

## Metrics Collected

This pipeline collects the following metrics:

### IPMI Metrics
- Temperature readings from IPMI sensors
- Fan speeds
- Power consumption
- Sensor states

### Node Metrics
- CPU usage
- Memory usage
- Disk space
- Network I/O

### DCGM (GPU) Metrics
- GPU temperature
- GPU utilization
- Memory usage
- Power usage

### Slurm Metrics
- Allocated nodes
- Idle nodes
- Running jobs
- Pending jobs

## Configuration Files

- `docker-compose.yml`: Defines the Docker containers for Prometheus, Telegraf, and InfluxDB
- `prometheus/prometheus.yml`: Configures Prometheus to scrape exporters
- `telegraf/telegraf.conf`: Configures Telegraf to collect metrics from Prometheus and forward to InfluxDB

## Maintenance

To check the status of the pipeline:
```bash
./check_pipeline.sh
```

To restart the pipeline:
```bash
docker-compose down
docker-compose up -d
```

## Troubleshooting

- Check container logs:
  ```bash
  docker logs exaarafpipeline-prometheus-1
  docker logs exaarafpipeline-telegraf-1
  docker logs exaarafpipeline-influxdb-1
  ```

- Validate Telegraf configuration:
  ```bash
  docker exec exaarafpipeline-telegraf-1 telegraf --test
  ```

- Check Prometheus targets:
  ```bash
  curl http://localhost:9099/api/v1/targets
  ```
