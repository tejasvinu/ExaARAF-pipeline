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
mkdir -p spark/apps
mkdir -p spark/data
mkdir -p telegraf
mkdir -p python_pipeline/logs

# Grant permissions if needed
chmod -R 777 spark/apps
chmod -R 777 spark/data
chmod -R 777 python_pipeline/logs
chmod -R 777 telegraf

# Create checkpoint directories with proper permissions
echo "Creating checkpoint directories..."
mkdir -p spark/checkpoints
chmod -R 777 spark/checkpoints

for topic in "ipmi_metrics" "node_metrics" "dcgm_metrics" "slurm_metrics"; do
    mkdir -p "spark/checkpoints/$topic"
    chmod -R 777 "spark/checkpoints/$topic"
done

# Start the containers
docker-compose up -d

echo "Waiting for services to be ready..."
sleep 20

# Check core services
for service in "prometheus" "zookeeper-1" "zookeeper-2" "zookeeper-3" "kafka-1" "kafka-2" "kafka-3" "influxdb" "telegraf"; do
    if ! check_container "$service"; then
        echo "Error: $service is not running. Please check docker-compose logs"
        exit 1
    fi
done

# Wait for Kafka to be fully ready
echo "Waiting for Kafka cluster to be ready..."
max_retries=30
retry_count=0
while ! docker exec exaarafpipeline-kafka-1-1 kafka-topics --bootstrap-server kafka-1:19092 --list > /dev/null 2>&1; do
    if [ $retry_count -ge $max_retries ]; then
        echo "Error: Kafka is not ready after $max_retries attempts"
        exit 1
    fi
    echo "Waiting for Kafka to be ready... (attempt $((retry_count + 1))/$max_retries)"
    sleep 10
    ((retry_count++))
done

# Wait for InfluxDB to be ready and setup monitoring
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

# Set up Python environment
echo "Setting up Python environment..."
cd python_pipeline

# Create and activate virtual environment
python -m venv venv
source venv/bin/activate || source venv/Scripts/activate

# Upgrade pip and install dependencies
python -m pip install --upgrade pip
pip install -r requirements.txt

# Start the Python pipeline in the background
echo "Starting Python pipeline..."
python main.py > logs/pipeline.log 2>&1 &
PIPELINE_PID=$!
echo "Python pipeline started with PID: $PIPELINE_PID"

# Wait briefly and check if the process is still running
sleep 10
if ps -p $PIPELINE_PID > /dev/null; then
    echo "Pipeline is running successfully"
    # Save PID for later management
    echo $PIPELINE_PID > python_pipeline/pipeline.pid
else
    echo "Error: Pipeline failed to start. Check logs/pipeline.log for details"
    exit 1
fi

echo "Setup complete! Monitor logs with:"
echo "tail -f python_pipeline/logs/pipeline.log"
