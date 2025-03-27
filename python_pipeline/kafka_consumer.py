import logging
import threading
import time
from kafka import KafkaConsumer
import json
from datetime import datetime, timezone
import os
from timestamp_utils import timestamp_manager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('consumer')

class MetricsConsumer:
    def __init__(self, bootstrap_servers='localhost:9092,localhost:9097,localhost:9095'):
        self.bootstrap_servers = bootstrap_servers
        self.consumers = {}
        logger.info(f"Initialized consumer with bootstrap servers: {bootstrap_servers}")

    def consume_metrics(self, metric_type):
        """Monitor Kafka metrics consumption - Telegraf handles the actual storage"""
        topic = f"{metric_type}_metrics"
        
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=f'metrics-monitor-{metric_type}',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                max_poll_records=100,
                max_poll_interval_ms=300000,
                session_timeout_ms=60000,
                heartbeat_interval_ms=20000
            )
            
            logger.info(f"Started monitoring {topic}")
            
            message_count = 0
            last_log_time = time.time()
            
            while True:
                try:
                    messages = consumer.poll(timeout_ms=1000)
                    for _, records in messages.items():
                        message_count += len(records)
                    
                    # Log metrics every minute
                    current_time = time.time()
                    if current_time - last_log_time >= 60:
                        logger.info(f"Monitored {message_count} messages from {topic} in the last minute")
                        message_count = 0
                        last_log_time = current_time
                        
                    time.sleep(0.1)
                    
                except Exception as e:
                    logger.error(f"Error monitoring {topic}: {str(e)}")
                    
        except Exception as e:
            logger.error(f"Error creating consumer for {topic}: {str(e)}")
            
    def run(self):
        """Start monitoring threads for all metric types"""
        threads = []
        
        for metric_type in ["ipmi", "node", "dcgm", "slurm"]:
            thread = threading.Thread(
                target=self.consume_metrics,
                args=(metric_type,),
                daemon=True
            )
            thread.start()
            threads.append(thread)
            logger.info(f"Started monitoring thread for {metric_type}")
            
        try:
            # Keep main thread alive
            while all(t.is_alive() for t in threads):
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Consumer monitoring stopped by user")

if __name__ == "__main__":
    consumer = MetricsConsumer()
    consumer.run()
