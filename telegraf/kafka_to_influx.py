#!/usr/bin/env python3
import sys
import json
import re
from datetime import datetime

def parse_labels(labels_str):
    """Parse the nested JSON string in labels field into a dict of tags"""
    try:
        # Remove escaped quotes and parse JSON
        cleaned_str = labels_str.replace('\\"', '"')
        if cleaned_str.startswith('"') and cleaned_str.endswith('"'):
            cleaned_str = cleaned_str[1:-1]
        return json.loads(cleaned_str)
    except Exception as e:
        return {"parse_error": "true"}

def process_message(line):
    """Process a single message from Kafka and convert to InfluxDB line protocol"""
    try:
        # Parse the JSON message
        data = json.loads(line)
        
        # Extract fields
        metric_name = data.get("name", "unknown_metric")
        timestamp = data.get("timestamp")
        
        # Handle NaN value - convert to string representation for InfluxDB
        value = data.get("value")
        if value == "NaN" or (isinstance(value, float) and (value != value)):  # NaN check
            value = "NaN"
        
        # Convert labels JSON string to tags
        labels_str = data.get("labels", "{}")
        labels = parse_labels(labels_str)
        
        # Add metric_type as a tag if available
        metric_type = data.get("metric_type")
        if metric_type:
            labels["metric_type"] = metric_type
        
        # Construct InfluxDB line protocol
        # Format: measurement,tag1=value1,tag2=value2 field1=value1,field2=value2 timestamp
        tags = ",".join([f"{k}={v}" for k, v in labels.items() if v is not None])
        tags_str = f",{tags}" if tags else ""
        
        # Convert timestamp to nanoseconds if it exists
        timestamp_ns = ""
        if timestamp:
            try:
                dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f%z")
                timestamp_ns = f" {int(dt.timestamp() * 1000000000)}"
            except Exception:
                pass
        
        # Return line in InfluxDB format
        return f"{metric_name}{tags_str} value={value}{timestamp_ns}"
    except Exception as e:
        return f"error,message=\"{str(e).replace('\"', '\\"')}\" value=\"{line.replace('\"', '\\"')}\""

if __name__ == "__main__":
    for line in sys.stdin:
        line = line.strip()
        if line:
            print(process_message(line))
            sys.stdout.flush()