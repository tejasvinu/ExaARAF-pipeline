import requests
import time
import json
import logging
import os
from datetime import datetime, timezone
from timestamp_utils import timestamp_manager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('scraper')

# Add file handler
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
log_file = os.path.join(log_dir, "scraper.log")
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)

class PrometheusMetricsScraper:
    def __init__(self, prometheus_url="http://localhost:9099"):  # Updated port to 9099
        self.prometheus_url = prometheus_url
        self.job_configs = {
            "ipmi": {
                "job": "ipmi-exporter",
                "target": "10.180.8.24:9290",
                "retry_count": 3,
                "timeout": 10
            },
            "node": {
                "job": "node-exporter",
                "target": "10.180.8.24:9100",
                "retry_count": 3,
                "timeout": 5
            },
            "dcgm": {
                "job": "dcgm-exporter",
                "target": "10.180.8.24:9400",
                "retry_count": 3,
                "timeout": 10
            },
            "slurm": {
                "job": "slurm-exporter",
                "target": "10.180.8.24:8080",
                "retry_count": 3,
                "timeout": 5
            }
        }
        self.last_scrape_info = {}
        self.session = self._create_session()
        logger.info(f"Initialized scraper with Prometheus URL: {prometheus_url}")

    def _create_session(self):
        """Create a requests session with retry configuration"""
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'PrometheusMetricsScraper/1.0',
            'Accept': 'application/json'
        })
        return session

    def _get_prometheus_timestamp(self, result):
        """Extract timestamp from Prometheus result"""
        try:
            # Get timestamp from Prometheus result
            prom_timestamp = float(result["value"][0])
            # Use timestamp manager to convert to UTC
            return timestamp_manager.from_unix_timestamp(prom_timestamp)
        except (KeyError, IndexError, ValueError) as e:
            logger.warning(f"Failed to extract timestamp: {str(e)}")
            # Fallback to current UTC time
            return timestamp_manager.get_current_timestamp()

    def _make_request(self, url, params, timeout, retries):
        """Make HTTP request with retry logic"""
        last_exception = None
        
        for attempt in range(retries):
            try:
                response = self.session.get(url, params=params, timeout=timeout)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                last_exception = e
                if attempt < retries - 1:
                    wait_time = (attempt + 1) * 2  # Exponential backoff
                    logger.warning(f"Request failed, attempt {attempt + 1}/{retries}. Retrying in {wait_time}s. Error: {str(e)}")
                    time.sleep(wait_time)
                    
        logger.error(f"Failed after {retries} attempts: {str(last_exception)}")
        raise last_exception

    def scrape_metrics(self, metric_type):
        """Scrape metrics for a specific exporter type with improved error handling"""
        if metric_type not in self.job_configs:
            logger.error(f"Unknown metric type: {metric_type}")
            return []

        job_config = self.job_configs[metric_type]
        job_name = job_config["job"]
        target = job_config["target"]
        timeout = job_config["timeout"]
        retry_count = job_config["retry_count"]
        
        try:
            # Query for all metrics at once using instant vector
            query_url = f"{self.prometheus_url}/api/v1/query"
            query = f'{{job="{job_name}",instance="{target}"}}'
            
            results = self._make_request(
                query_url,
                params={"query": query},
                timeout=timeout,
                retries=retry_count
            )
            
            if results["status"] != "success":
                logger.error(f"Failed to fetch metrics for {metric_type}: {results}")
                return []
                
            metric_results = []
            
            for result in results.get("data", {}).get("result", []):
                try:
                    # Get timestamp from Prometheus
                    timestamp = self._get_prometheus_timestamp(result)
                    
                    # Extract metric name and ensure it's valid
                    metric_name = result["metric"].get("__name__")
                    if not metric_name:
                        continue
                    
                    # Parse and validate the value
                    try:
                        value = float(result["value"][1])
                    except (IndexError, ValueError, TypeError):
                        logger.warning(f"Invalid value for metric {metric_name}")
                        continue
                    
                    metric_info = {
                        "name": metric_name,
                        "timestamp": timestamp.isoformat(),
                        "value": value,
                        "labels": json.dumps(result["metric"]),
                        "metric_type": metric_type
                    }
                    metric_results.append(metric_info)
                except Exception as e:
                    logger.warning(f"Could not process result: {str(e)}")
                    continue
            
            # Update last scrape information with detailed stats
            self.last_scrape_info[metric_type] = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "metric_count": len(metric_results),
                "sample_metrics": [m["name"] for m in metric_results[:5]],
                "value_ranges": {
                    "min": min([m["value"] for m in metric_results]) if metric_results else None,
                    "max": max([m["value"] for m in metric_results]) if metric_results else None
                }
            }
            
            # Log detailed scrape information
            logger.info(f"Scrape completed for {metric_type}:")
            logger.info(f"  - Metrics collected: {len(metric_results)}")
            if metric_results:
                logger.info(f"  - Sample metrics: {', '.join(self.last_scrape_info[metric_type]['sample_metrics'])}")
                logger.info(f"  - Value range: {self.last_scrape_info[metric_type]['value_ranges']}")
            
            return metric_results
            
        except Exception as e:
            logger.error(f"Error scraping {metric_type} metrics: {str(e)}")
            return []

    def _fetch_metric_value(self, metric_name, job_name=None):
        """Fetch the current value for a specific metric"""
        query_url = f"{self.prometheus_url}/api/v1/query"
        query = metric_name
        if job_name:
            query = f'{metric_name}{{job="{job_name}"}}'
        
        try:
            results = self._make_request(
                query_url,
                params={"query": query},
                timeout=5,
                retries=2
            )
            
            if results["status"] != "success":
                logger.error(f"Failed to fetch {metric_name}: {results}")
                return []
                
            metric_results = []
            
            for result in results.get("data", {}).get("result", []):
                try:
                    # Get timestamp from Prometheus
                    timestamp = self._get_prometheus_timestamp(result)
                    
                    # Validate value
                    try:
                        value = float(result["value"][1])
                    except (IndexError, ValueError, TypeError):
                        continue
                    
                    metric_info = {
                        "name": metric_name,
                        "timestamp": timestamp.isoformat(),
                        "value": value,
                        "labels": json.dumps(result["metric"])
                    }
                    metric_results.append(metric_info)
                except Exception as e:
                    logger.warning(f"Could not process result for {metric_name}: {str(e)}")
                
            return metric_results
                
        except Exception as e:
            logger.error(f"Error fetching metric {metric_name}: {str(e)}")
            return []

    def get_last_scrape_summary(self):
        """Get a detailed summary of the most recent scrapes for all exporters"""
        summary = []
        for metric_type, info in self.last_scrape_info.items():
            summary.append(f"\nExporter: {metric_type}")
            summary.append(f"Last scrape time: {info['timestamp']}")
            summary.append(f"Metrics collected: {info['metric_count']}")
            summary.append(f"Sample metrics: {', '.join(info['sample_metrics'])}")
            if 'value_ranges' in info:
                summary.append(f"Value ranges: Min={info['value_ranges']['min']}, Max={info['value_ranges']['max']}")
        return "\n".join(summary)

if __name__ == "__main__":
    # Test the scraper
    scraper = PrometheusMetricsScraper()
    for metric_type in ["ipmi", "node", "dcgm", "slurm"]:
        metrics = scraper.scrape_metrics(metric_type)
        print(f"{metric_type}: {len(metrics)} metrics")
        if metrics:
            print(f"Sample: {metrics[0]}")
    
    # Print summary of last scrapes
    print("\nLast Scrape Summary:")
    print(scraper.get_last_scrape_summary())
