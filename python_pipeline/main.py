import logging
import threading
import time
import signal
import sys
import psutil
import os
from kafka_producer import MetricsProducer
from kafka_consumer import MetricsConsumer

# Configure logging with rotating file handler
from logging.handlers import RotatingFileHandler
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler(
            os.path.join(log_dir, "pipeline.log"),
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        ),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('pipeline')

class MetricsPipeline:
    def __init__(self):
        self.producer = None
        self.consumer = None
        self.producer_thread = None
        self.consumer_thread = None
        self.shutdown_event = threading.Event()
        self.health_check_thread = None
        
    def monitor_process_health(self):
        """Monitor process health metrics"""
        process = psutil.Process()
        while not self.shutdown_event.is_set():
            try:
                cpu_percent = process.cpu_percent(interval=1)
                memory_info = process.memory_info()
                thread_count = process.num_threads()
                
                logger.info(
                    f"Process health - CPU: {cpu_percent}%, "
                    f"Memory: {memory_info.rss / 1024 / 1024:.1f}MB, "
                    f"Threads: {thread_count}"
                )
                
                # Check thread health
                if self.producer_thread and not self.producer_thread.is_alive():
                    logger.error("Producer thread died, initiating shutdown")
                    self.shutdown()
                    break
                    
                if self.consumer_thread and not self.consumer_thread.is_alive():
                    logger.error("Consumer thread died, initiating shutdown")
                    self.shutdown()
                    break
                    
            except Exception as e:
                logger.error(f"Error monitoring process health: {str(e)}")
            
            time.sleep(30)  # Check every 30 seconds
            
    def run_producer(self):
        """Run the metrics producer"""
        try:
            self.producer = MetricsProducer()
            self.producer.run_continuously(interval_seconds=15)
        except Exception as e:
            logger.error(f"Producer error: {str(e)}")
            self.shutdown()
            
    def run_consumer(self):
        """Run the metrics consumer"""
        try:
            self.consumer = MetricsConsumer()
            self.consumer.run()
        except Exception as e:
            logger.error(f"Consumer error: {str(e)}")
            self.shutdown()
            
    def shutdown(self):
        """Gracefully shutdown the pipeline"""
        logger.info("Initiating pipeline shutdown...")
        self.shutdown_event.set()
        
        # Stop producer and consumer
        if self.producer:
            try:
                self.producer.producer.close(timeout=30)
            except Exception as e:
                logger.error(f"Error closing producer: {str(e)}")
                
        if self.consumer:
            try:
                if hasattr(self.consumer, 'cluster'):
                    self.consumer.cluster.shutdown()
            except Exception as e:
                logger.error(f"Error closing consumer: {str(e)}")
                
        logger.info("Pipeline shutdown complete")
        sys.exit(0)
        
    def run(self):
        """Start the pipeline"""
        logger.info("Starting metrics pipeline")
        
        # Set up signal handlers
        signal.signal(signal.SIGTERM, lambda signo, frame: self.shutdown())
        signal.signal(signal.SIGINT, lambda signo, frame: self.shutdown())
        
        try:
            # Start producer and consumer threads
            self.producer_thread = threading.Thread(
                target=self.run_producer,
                daemon=True,
                name="ProducerThread"
            )
            self.consumer_thread = threading.Thread(
                target=self.run_consumer,
                daemon=True,
                name="ConsumerThread"
            )
            
            self.producer_thread.start()
            self.consumer_thread.start()
            
            # Start health monitoring
            self.health_check_thread = threading.Thread(
                target=self.monitor_process_health,
                daemon=True,
                name="HealthCheckThread"
            )
            self.health_check_thread.start()
            
            logger.info("Pipeline started successfully")
            
            # Keep main thread alive and monitor threads
            while not self.shutdown_event.is_set():
                time.sleep(1)
                
        except Exception as e:
            logger.error(f"Error in pipeline: {str(e)}")
            self.shutdown()
            
if __name__ == "__main__":
    pipeline = MetricsPipeline()
    pipeline.run()
