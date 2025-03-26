#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if pipeline process is running
check_pipeline() {
    if [ -f "python_pipeline/pipeline.pid" ]; then
        PID=$(cat python_pipeline/pipeline.pid)
        if ps -p $PID > /dev/null; then
            echo -e "${GREEN}Pipeline is running (PID: $PID)${NC}"
            return 0
        else
            echo -e "${RED}Pipeline process not found, but PID file exists${NC}"
            return 1
        fi
    else
        echo -e "${RED}Pipeline PID file not found${NC}"
        return 1
    fi
}

# Check Docker container status
check_containers() {
    echo -e "\n${YELLOW}Checking container status...${NC}"
    
    services=("prometheus" "zookeeper-1" "zookeeper-2" "zookeeper-3" 
             "kafka-1" "kafka-2" "kafka-3" 
             "cassandra-1" "cassandra-2" "cassandra-3")
             
    all_healthy=true
    
    for service in "${services[@]}"; do
        status=$(docker ps -f name=exaarafpipeline-$service-1 --format "{{.Status}}")
        if [ -n "$status" ]; then
            if [[ $status == *"healthy"* ]]; then
                echo -e "${GREEN}$service: $status${NC}"
            else
                echo -e "${YELLOW}$service: $status${NC}"
            fi
        else
            echo -e "${RED}$service: Not running${NC}"
            all_healthy=false
        fi
    done
    
    if [ "$all_healthy" = false ]; then
        return 1
    fi
    return 0
}

# Show recent logs
show_logs() {
    echo -e "\n${YELLOW}Recent pipeline logs:${NC}"
    if [ -f "python_pipeline/logs/pipeline.log" ]; then
        tail -n 50 python_pipeline/logs/pipeline.log
    else
        echo -e "${RED}Log file not found${NC}"
    fi
}

# Check Kafka topics
check_kafka() {
    echo -e "\n${YELLOW}Checking Kafka topics...${NC}"
    docker exec exaarafpipeline-kafka-1-1 kafka-topics --bootstrap-server kafka-1:19092 --list
    
    echo -e "\n${YELLOW}Topic details:${NC}"
    topics=("ipmi_metrics" "node_metrics" "dcgm_metrics" "slurm_metrics")
    for topic in "${topics[@]}"; do
        echo -e "\n${YELLOW}$topic:${NC}"
        docker exec exaarafpipeline-kafka-1-1 kafka-topics \
            --bootstrap-server kafka-1:19092 \
            --describe \
            --topic $topic
    done
}

# Check Cassandra status
check_cassandra() {
    echo -e "\n${YELLOW}Checking Cassandra cluster status...${NC}"
    docker exec exaarafpipeline-cassandra-1-1 nodetool status
}

# Main execution
echo -e "${YELLOW}Pipeline Status Check${NC}"
echo "========================="

check_pipeline
pipeline_status=$?

check_containers
containers_status=$?

if [ $# -eq 0 ]; then
    # No arguments, show basic status
    if [ $pipeline_status -eq 0 ] && [ $containers_status -eq 0 ]; then
        echo -e "\n${GREEN}Pipeline is healthy${NC}"
    else
        echo -e "\n${RED}Pipeline has issues${NC}"
    fi
else
    # Process arguments
    for arg in "$@"; do
        case $arg in
            --logs)
                show_logs
                ;;
            --kafka)
                check_kafka
                ;;
            --cassandra)
                check_cassandra
                ;;
            --all)
                show_logs
                check_kafka
                check_cassandra
                ;;
            *)
                echo -e "${RED}Unknown option: $arg${NC}"
                echo "Available options: --logs, --kafka, --cassandra, --all"
                exit 1
                ;;
        esac
    done
fi