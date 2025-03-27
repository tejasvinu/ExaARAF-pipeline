#!/bin/bash

echo "Checking ExaARAF simplified pipeline status..."

# Check containers
echo "Checking container status:"
for service in "prometheus" "influxdb" "telegraf"; do
    if docker ps | grep -q "exaarafpipeline-$service-1"; then
        echo "✓ $service: Running"
    else
        echo "✗ $service: Not running"
    fi
done

# Check Prometheus targets
echo -e "\nChecking Prometheus targets:"
response=$(curl -s "http://localhost:9099/api/v1/targets")
up=$(echo "$response" | grep -o '"health":"up"' | wc -l)
down=$(echo "$response" | grep -o '"health":"down"' | wc -l)
echo "$up targets up, $down targets down"

# Check Telegraf configuration
echo -e "\nChecking Telegraf config:"
docker exec exaarafpipeline-telegraf-1 telegraf --test --config /etc/telegraf/telegraf.conf --test-wait 2

echo -e "\nPipeline Summary:"
echo "Simplified pipeline deployed: Prometheus → Telegraf → InfluxDB"
echo "Access InfluxDB UI: http://localhost:8086"
echo "Access Prometheus UI: http://localhost:9099"