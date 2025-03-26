import time
import logging
import sys
import pandas as pd
import numpy as np
import random
import datetime
import os
from collections import defaultdict

if sys.version_info[0] < 3:
    sys.exit("This script requires Python 3.")

# Configure logging for detailed output
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Ensure output directory exists
OUTPUT_DIR = "analysis_results"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def generate_web_logs(num_entries=50000):
    """Generate realistic web server log data."""
    logging.info(f"Generating {num_entries} web server log entries...")
    
    # Define possible values for log entries
    ip_addresses = [f"192.168.1.{i}" for i in range(1, 100)] + [f"10.0.0.{i}" for i in range(1, 50)]
    endpoints = ["/home", "/api/data", "/login", "/logout", "/profile", "/settings", 
                "/products", "/cart", "/checkout", "/admin", "/static/css", "/static/js"]
    http_methods = ["GET", "POST", "PUT", "DELETE"]
    status_codes = [200, 200, 200, 200, 201, 204, 400, 401, 403, 404, 500]
    user_agents = ["Mozilla/5.0", "Chrome/91.0", "Safari/605.1", "Edge/91.0", "Mobile/15E148"]
    
    # Current timestamp for log generation
    end_time = datetime.datetime.now()
    start_time = end_time - datetime.timedelta(hours=1)
    
    # Generate log entries
    logs = []
    for _ in range(num_entries):
        timestamp = start_time + (end_time - start_time) * random.random()
        ip = random.choice(ip_addresses)
        endpoint = random.choice(endpoints)
        method = random.choice(http_methods)
        status = random.choice(status_codes)
        response_time = random.uniform(0.01, 2.0) if status < 500 else random.uniform(1.0, 10.0)
        bytes_sent = random.randint(500, 5000)
        user_agent = random.choice(user_agents)
        
        logs.append({
            'timestamp': timestamp,
            'ip_address': ip,
            'endpoint': endpoint,
            'method': method,
            'status_code': status,
            'response_time': response_time,
            'bytes_sent': bytes_sent,
            'user_agent': user_agent
        })
    
    df = pd.DataFrame(logs)
    logging.info("Web log generation complete.")
    return df

def analyze_logs(df):
    """Analyze web logs to extract operational metrics."""
    logging.info("Starting log analysis...")
    
    # Add time dimensions for analysis
    df['minute'] = df['timestamp'].dt.floor('min')
    
    # Calculate metrics
    metrics = {
        # Traffic metrics
        'total_requests': len(df),
        'requests_per_endpoint': df.groupby('endpoint').size().to_dict(),
        'requests_per_minute': df.groupby('minute').size().to_dict(),
        
        # Performance metrics
        'avg_response_time': df['response_time'].mean(),
        'p95_response_time': df['response_time'].quantile(0.95),
        'response_time_by_endpoint': df.groupby('endpoint')['response_time'].mean().to_dict(),
        
        # Error metrics
        'error_rate': (df['status_code'] >= 400).mean(),
        'status_code_distribution': df.groupby('status_code').size().to_dict(),
        
        # User metrics
        'unique_ips': df['ip_address'].nunique(),
        'top_users': df['ip_address'].value_counts().head(10).to_dict()
    }
    
    # Detect anomalies (very basic example)
    slow_endpoints = df.groupby('endpoint')['response_time'].mean()
    metrics['slow_endpoints'] = slow_endpoints[slow_endpoints > 1.0].to_dict()
    
    # Error spike detection
    error_by_minute = df[df['status_code'] >= 400].groupby('minute').size()
    if not error_by_minute.empty:
        metrics['error_spikes'] = error_by_minute[error_by_minute > error_by_minute.mean() + 2*error_by_minute.std()].to_dict()
    else:
        metrics['error_spikes'] = {}
    
    logging.info("Log analysis complete. Processed {} log entries.".format(len(df)))
    return metrics

def save_analysis_results(metrics, output_file):
    """Save the analysis results to a file."""
    logging.info(f"Saving analysis results to {output_file}...")
    
    with open(output_file, 'w') as f:
        f.write("=== WEB LOG ANALYSIS RESULTS ===\n")
        f.write(f"Generated at: {datetime.datetime.now()}\n\n")
        
        f.write("=== TRAFFIC METRICS ===\n")
        f.write(f"Total Requests: {metrics['total_requests']}\n")
        f.write("Top Endpoints:\n")
        for endpoint, count in sorted(metrics['requests_per_endpoint'].items(), key=lambda x: x[1], reverse=True)[:5]:
            f.write(f"  {endpoint}: {count} requests\n")
            
        f.write("\n=== PERFORMANCE METRICS ===\n")
        f.write(f"Average Response Time: {metrics['avg_response_time']:.3f}s\n")
        f.write(f"95th Percentile Response Time: {metrics['p95_response_time']:.3f}s\n")
        
        f.write("\n=== ERROR METRICS ===\n")
        f.write(f"Error Rate: {metrics['error_rate']*100:.2f}%\n")
        f.write("Status Code Distribution:\n")
        for code, count in sorted(metrics['status_code_distribution'].items()):
            f.write(f"  {code}: {count} requests\n")
            
        f.write("\n=== ANOMALIES ===\n")
        if metrics['slow_endpoints']:
            f.write("Slow Endpoints:\n")
            for endpoint, time in metrics['slow_endpoints'].items():
                f.write(f"  {endpoint}: {time:.3f}s\n")
        
        if metrics['error_spikes']:
            f.write("Error Spikes:\n")
            for minute, count in metrics['error_spikes'].items():
                f.write(f"  {minute}: {count} errors\n")
    
    logging.info("Analysis results saved successfully.")

def heavy_computation(iteration=1):
    logging.info(f"Performing heavy computation for iteration {iteration}...")
    size = 5000
    a = np.random.rand(size, size)
    b = np.random.rand(size, size)
    # Run multiple multiplications to hit the CPU harder
    for i in range(3):
        result = np.dot(a, b)
        logging.info(f"Iteration {iteration} sub-computation {i+1} complete. Sum: {result.sum():.2f}")

def main():
    start_time = time.time()
    iteration = 0
    while time.time() - start_time < 600:  # 10 minutes
        iteration += 1
        heavy_computation(iteration)
        time.sleep(1)
    logging.info("Heavy CPU Job completed successfully.")

if __name__ == "__main__":
    main()