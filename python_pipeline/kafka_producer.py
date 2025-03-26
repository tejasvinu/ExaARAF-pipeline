import os
from kafka import KafkaProducer
import json
import logging
import time
from scraper import PrometheusMetricsScraper
from datetime import datetime
import pytz
from timestamp_utils import timestamp_manager

# Set timezone to IST
os.environ['TZ'] = 'Asia/Kolkata'
time.tzset()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S %Z'
)
logger = logging.getLogger('producer')

class MetricsProducer:
    def __init__(self, bootstrap_servers='localhost:9092,localhost:9093,localhost:9095'):
        # Configure Kafka producer with reliability settings
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=self._datetime_handler).encode('utf-8'),
            retries=5,
            acks='all',  # Wait for all replicas
            compression_type='lz4',
            batch_size=16384,
            linger_ms=100,  # Wait to batch more messages
            max_in_flight_requests_per_connection=5,
            max_request_size=5000000,
            request_timeout_ms=30000,
            retry_backoff_ms=500
        )
        self.scraper = PrometheusMetricsScraper()
        logger.info(f"Initialized Kafka producer with bootstrap servers: {bootstrap_servers}")
        
    def _datetime_handler(self, obj):
        """Convert datetime objects to ISO format string in UTC"""
        if isinstance(obj, datetime):
            # Ensure UTC timezone and format
            return timestamp_manager.format_timestamp(obj, timezone=timestamp_manager.utc_tz)
        return obj
        
    def produce_metrics(self, metric_type):
        """Scrape and produce metrics for a specific type with retries"""
        topic = f"{metric_type}_metrics"
        max_retries = 3
        retry_delay = 1.0  # seconds
        
        for attempt in range(max_retries):
            try:
                metrics = self.scraper.scrape_metrics(metric_type)
                
                if not metrics:
                    logger.warning(f"No metrics found for {metric_type}")
                    return 0
                
                count = 0
                futures = []  # Track futures for error handling
                
                for metric in metrics:
                    try:
                        future = self.producer.send(topic, value=metric)
                        futures.append(future)
                        count += 1
                    except Exception as e:
                        logger.error(f"Error sending metric to Kafka: {str(e)}")
                
                # Wait for all messages to be sent
                for future in futures:
                    try:
                        future.get(timeout=10)  # Wait up to 10 seconds
                    except Exception as e:
                        logger.error(f"Error in message delivery: {str(e)}")
                        count -= 1
                
                self.producer.flush()
                logger.info(f"Successfully sent {count} metrics to topic {topic}")
                return count
                
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (attempt + 1)  # Exponential backoff
                    logger.warning(f"Attempt {attempt + 1} failed for {topic}. Retrying in {wait_time}s. Error: {str(e)}")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to produce metrics for {topic} after {max_retries} attempts")
                    return 0
        
    def run_once(self):
        """Run one collection cycle for all metric types"""
        total = 0
        failed_types = []
        
        for metric_type in ["ipmi", "node", "dcgm", "slurm"]:
            try:
                count = self.produce_metrics(metric_type)
                if count > 0:
                    total += count
                else:
                    failed_types.append(metric_type)
            except Exception as e:
                logger.error(f"Error processing {metric_type}: {str(e)}")
                failed_types.append(metric_type)
        
        if failed_types:
            logger.warning(f"Failed to collect metrics for: {', '.join(failed_types)}")
        logger.info(f"Total metrics sent: {total}")
        return total
        
    def run_continuously(self, interval_seconds=15):
        """Run the producer in a continuous loop with the specified interval"""
        logger.info(f"Starting continuous production every {interval_seconds} seconds")
        consecutive_failures = 0
        max_failures = 5
        
        try:
            while True:
                start_time = time.time()
                try:
                    total = self.run_once()
                    if total > 0:
                        consecutive_failures = 0
                    else:
                        consecutive_failures += 1
                        
                    if consecutive_failures >= max_failures:
                        logger.error(f"Stopping after {max_failures} consecutive failures")
                        break
                        
                except Exception as e:
                    logger.error(f"Error in production cycle: {str(e)}")
                    consecutive_failures += 1
                    if consecutive_failures >= max_failures:
                        logger.error(f"Stopping after {max_failures} consecutive failures")
                        break
                
                # Sleep for the remaining time in the interval
                elapsed = time.time() - start_time
                sleep_time = max(0, interval_seconds - elapsed)
                if sleep_time > 0:
                    logger.info(f"Waiting {sleep_time:.1f} seconds until next collection")
                    time.sleep(sleep_time)
                    
        except KeyboardInterrupt:
            logger.info("Producer stopped by user")
        finally:
            try:
                self.producer.flush(timeout=30)  # Give extra time for final flush
                self.producer.close(timeout=30)
            except Exception as e:
                logger.error(f"Error closing producer: {str(e)}")
            
if __name__ == "__main__":
    producer = MetricsProducer()
    producer.run_continuously()
