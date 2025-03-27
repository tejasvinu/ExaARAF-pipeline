#!/bin/bash

# Function to check if a container is running
check_container() {
    local container_name="exaarafpipeline-$1-1"
    echo "Checking $container_name..."
    if [ "$(docker ps -q -f name=$container_name)" ]; then
        echo "$1 is running"
        return 0
    else
        echo "$1 is not running"
        return 1
    fi
}

# Create required directories
mkdir -p prometheus/
mkdir -p telegraf

# Grant permissions if needed
chmod -R 777 telegraf

# Start the containers
echo "Starting Docker containers..."
docker-compose up -d

echo "Waiting for services to be ready..."
sleep 20

# Check core services - no Kafka/Zookeeper services in our simplified architecture
for service in "prometheus" "influxdb" "telegraf"; do
    if ! check_container "$service"; then
        echo "Error: $service is not running. Please check docker-compose logs"
        exit 1
    fi
done

# Wait for InfluxDB to be ready
echo "Waiting for InfluxDB to be ready..."
max_retries=10
retry_count=0
while ! curl -s "http://localhost:8086/health" > /dev/null; do
    if [ $retry_count -ge $max_retries ]; then
        echo "Error: InfluxDB is not ready after $max_retries attempts"
        exit 1
    fi
    echo "Waiting for InfluxDB to be ready... (attempt $((retry_count + 1))/$max_retries)"
    sleep 10
    ((retry_count++))
done

echo "Checking Telegraf status..."
docker exec exaarafpipeline-telegraf-1 telegraf --test

echo "Checking Prometheus status..."
curl -s "http://localhost:9099/-/healthy" || echo "Warning: Prometheus health check failed"

echo "Setup complete! You can now view metrics in InfluxDB at http://localhost:8086"
echo "Metrics are being collected from Prometheus, processed by Telegraf, and stored in InfluxDB."
