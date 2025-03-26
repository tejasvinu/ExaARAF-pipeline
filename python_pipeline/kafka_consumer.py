from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, ConsistencyLevel, SimpleStatement
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy, RoundRobinPolicy
from cassandra.concurrent import execute_concurrent_with_args
import json
import logging
import time
import threading
from datetime import datetime, timezone
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue, Empty
import multiprocessing
from timestamp_utils import timestamp_manager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('consumer')

class MetricsConsumer:
    def __init__(self, bootstrap_servers='localhost:9092,localhost:9097,localhost:9095', cassandra_host='localhost'):
        self.bootstrap_servers = bootstrap_servers
        self.cassandra_host = cassandra_host
        self.consumers = {}
        self.locks = {}
        
        # Create thread pools for parallel processing
        cpu_count = multiprocessing.cpu_count()
        self.worker_pool = ThreadPoolExecutor(max_workers=cpu_count * 2)
        self.cassandra_pool = ThreadPoolExecutor(max_workers=cpu_count)
        
        # Configure load balancing policy with token awareness
        load_balancing_policy = TokenAwarePolicy(RoundRobinPolicy())
        
        try:
            # Use Cluster with optimized configuration for 3-node setup
            self.cluster = Cluster(
                [cassandra_host],
                protocol_version=5,
                compression=True,
                load_balancing_policy=load_balancing_policy,
                control_connection_timeout=20,
                connect_timeout=20,
                executor_threads=cpu_count * 2,
                metrics_enabled=True,
                connection_class=None  # Let driver choose optimal connection
            )
        except Exception as e:
            logger.warning(f"Failed to initialize Cassandra cluster with metrics: {str(e)}")
            # Retry without metrics
            self.cluster = Cluster(
                [cassandra_host],
                protocol_version=5,
                compression=True,
                load_balancing_policy=load_balancing_policy,
                control_connection_timeout=20,
                connect_timeout=20,
                executor_threads=cpu_count * 2,
                metrics_enabled=False
            )
        
        self.session = self.cluster.connect()
        
        # Optimize session for high throughput with 3-node setup
        self.session.default_timeout = 30
        self.session.default_fetch_size = 1000
        self.session.default_consistency_level = ConsistencyLevel.LOCAL_QUORUM
        self.session.default_timestamp = int(time.time() * 1000)
        
        # Ensure keyspace exists with proper replication
        self.session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS metrics 
            WITH replication = {
                'class': 'SimpleStrategy', 
                'replication_factor': 3
            }
            """
        )
        self.session.set_keyspace('metrics')
        
        # Initialize tables
        self._create_tables()
        
        # Initialize locks for each metric type
        for metric_type in ["ipmi", "node", "dcgm", "slurm"]:
            self.locks[metric_type] = threading.Lock()
        
        # Configure adaptive batch sizes based on metric type
        self.batch_configs = {
            "node": {
                "batch_size": 50,  # Reduced from 100 to stay under 5120 byte limit
                "chunk_size": 10,  # Reduced from 25
                "max_concurrent": cpu_count
            },
            "default": {
                "batch_size": 50,
                "chunk_size": 10,
                "max_concurrent": cpu_count
            }
        }
        
        logger.info(f"Connected to Cassandra cluster at {cassandra_host}")
        
    def _get_batch_config(self, metric_type):
        """Get batch configuration for metric type"""
        return self.batch_configs.get(metric_type, self.batch_configs["default"])

    def _create_tables(self):
        """Create tables for each metric type if they don't exist"""
        for metric_type in ["ipmi", "node", "dcgm", "slurm"]:
            self.session.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {metric_type}_metrics (
                    metric_date date,
                    window_start timestamp,
                    window_end timestamp,
                    name text,
                    labels text,
                    avg_value double,
                    max_value double,
                    min_value double,
                    PRIMARY KEY ((metric_date, name), window_start)
                ) WITH CLUSTERING ORDER BY (window_start DESC)
                """
            )
        logger.info("Tables created/verified in Cassandra")
            
    def _process_metrics_parallel(self, messages, metric_type):
        """Process metrics in parallel using thread pools"""
        if not messages:
            return
            
        batch_config = self._get_batch_config(metric_type)
        metrics_queue = Queue()
        futures = []
        
        # Group messages into processing chunks
        for message in messages:
            metrics_queue.put(message)
        
        # Process chunks in parallel
        while not metrics_queue.empty():
            chunk = []
            try:
                for _ in range(batch_config["chunk_size"]):
                    if not metrics_queue.empty():
                        chunk.append(metrics_queue.get_nowait())
                if chunk:
                    future = self.worker_pool.submit(self._process_metric_chunk, chunk, metric_type)
                    futures.append(future)
            except Empty:
                pass
        
        # Wait for all processing to complete
        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    self._store_metrics_batch(result, metric_type)
            except Exception as e:
                logger.error(f"Error processing chunk: {str(e)}")

    def _process_metric_chunk(self, messages, metric_type):
        """Process a chunk of metrics"""
        metrics_to_store = []
        
        for message in messages:
            try:
                metric = message.value
                # Use centralized timestamp management
                timestamp = timestamp_manager.parse_timestamp(
                    metric.get('timestamp'), 
                    timestamp_manager.get_current_timestamp()
                )
                # Store in UTC for consistency
                timestamp_utc = timestamp_manager.ensure_utc(timestamp)
                metric_date = timestamp_utc.date()
                
                metrics_to_store.append({
                    'metric_date': metric_date,
                    'window_start': timestamp_utc,
                    'window_end': timestamp_utc,
                    'name': metric['name'],
                    'labels': metric.get('labels', '{}'),
                    'value': float(metric['value'])
                })
                
            except (ValueError, KeyError, AttributeError) as e:
                logger.error(f"Error processing metric: {str(e)}")
                continue
        
        return metrics_to_store

    def _store_metrics_batch(self, metrics, metric_type):
        """Store metrics using parallel execution for large batches"""
        if not metrics:
            return
            
        batch_config = self._get_batch_config(metric_type)
        # Use fully qualified table name
        prepared = self.session.prepare(
            f"""
            INSERT INTO metrics.{metric_type}_metrics 
            (metric_date, window_start, window_end, name, labels, avg_value, max_value, min_value)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
        )
        
        statements_and_params = []
        
        for metric in metrics:
            params = (
                metric['metric_date'],
                metric['window_start'],
                metric['window_end'],
                metric['name'],
                metric['labels'],
                metric['value'],  # Use actual value for all fields
                metric['value'],
                metric['value']
            )
            statements_and_params.append((prepared, params))
        
        # Execute in parallel with proper concurrency control
        if len(statements_and_params) > batch_config["batch_size"]:
            results = execute_concurrent_with_args(
                self.session,
                prepared,
                [params for _, params in statements_and_params],
                concurrency=batch_config["max_concurrent"]
            )
        else:
            # Use batch for smaller sets
            batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
            for _, params in statements_and_params:
                batch.add(prepared, params)
            self.session.execute(batch)
        
        logger.info(f"Stored {len(statements_and_params)} metrics for {metric_type}")

    def _estimate_batch_size(self, batch_items):
        """Roughly estimate batch size in bytes to avoid Cassandra warnings"""
        # Conservative estimate: ~200 bytes per batch item (actual size depends on labels length)
        return len(batch_items) * 200

    def _execute_batch(self, prepared, batch_items):
        """Execute a single batch of items with retry logic"""
        if not batch_items:
            return
            
        max_retries = 3
        retry_delay = 1.0  # seconds
        
        for attempt in range(max_retries):
            try:
                # For very small batches, use individual inserts
                if len(batch_items) <= 3:
                    for item in batch_items:
                        self.session.execute(prepared, item)
                else:
                    # Use batch for multiple items
                    batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
                    for item in batch_items:
                        batch.add(prepared, item)
                    self.session.execute(batch)
                return  # Success, exit the retry loop
                
            except Exception as e:
                if attempt < max_retries - 1:  # Don't sleep on the last attempt
                    time.sleep(retry_delay * (attempt + 1))  # Exponential backoff
                else:
                    logger.error(f"Failed to execute batch after {max_retries} attempts: {str(e)}")
                    raise  # Re-raise the last exception

    def _parse_timestamp(self, timestamp_str, default_time):
        """Parse timestamp consistently"""
        if not timestamp_str:
            return default_time.replace(tzinfo=None)
            
        try:
            if isinstance(timestamp_str, str):
                # Handle various timestamp formats
                if 'Z' in timestamp_str:
                    ts = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                elif '+' in timestamp_str or '-' in timestamp_str:
                    ts = datetime.fromisoformat(timestamp_str)
                else:
                    ts = datetime.fromisoformat(timestamp_str)
                    ts = ts.replace(tzinfo=timezone.utc)
                return ts.astimezone(timezone.utc).replace(tzinfo=None)
            else:
                # Handle datetime objects
                if timestamp_str.tzinfo is None:
                    return timestamp_str.replace(tzinfo=timezone.utc).replace(tzinfo=None)
                return timestamp_str.astimezone(timezone.utc).replace(tzinfo=None)
        except (ValueError, AttributeError):
            return default_time.replace(tzinfo=None)
            
    def consume_metrics(self, metric_type):
        """Consume metrics with parallel processing"""
        topic = f"{metric_type}_metrics"
        batch_config = self._get_batch_config(metric_type)
        
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=f'metrics-processor-{metric_type}',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                max_poll_records=batch_config["batch_size"] * 2,
                max_poll_interval_ms=300000,  # Increased for larger batches
                session_timeout_ms=60000,
                heartbeat_interval_ms=20000
            )
            
            logger.info(f"Started consuming from {topic}")
            
            while True:
                try:
                    messages = consumer.poll(timeout_ms=1000)
                    for _, records in messages.items():
                        if records:
                            self._process_metrics_parallel(records, metric_type)
                    time.sleep(0.1)  # Reduced sleep time
                except Exception as e:
                    logger.error(f"Error in consumer for {topic}: {str(e)}")
                    
        except Exception as e:
            logger.error(f"Error creating consumer for {topic}: {str(e)}")
            
    def run(self):
        """Start consumers for all metric types in separate threads"""
        threads = []
        
        for metric_type in ["ipmi", "node", "dcgm", "slurm"]:
            thread = threading.Thread(
                target=self.consume_metrics,
                args=(metric_type,),
                daemon=True
            )
            thread.start()
            threads.append(thread)
            logger.info(f"Started consumer thread for {metric_type}")
            
        try:
            # Keep main thread alive
            while all(t.is_alive() for t in threads):
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Consumer stopped by user")
        finally:
            if self.cluster:
                self.cluster.shutdown()

if __name__ == "__main__":
    consumer = MetricsConsumer()
    consumer.run()
