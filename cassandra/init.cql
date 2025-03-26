CREATE KEYSPACE IF NOT EXISTS metrics 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE metrics;

CREATE TABLE IF NOT EXISTS ipmi_metrics (
    metric_date date,
    window_start timestamp,
    window_end timestamp,
    name text,
    labels text,
    avg_value double,
    max_value double,
    min_value double,
    PRIMARY KEY ((metric_date, name), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

CREATE TABLE IF NOT EXISTS node_metrics (
    metric_date date,
    window_start timestamp,
    window_end timestamp,
    name text,
    labels text,
    avg_value double,
    max_value double,
    min_value double,
    PRIMARY KEY ((metric_date, name), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

CREATE TABLE IF NOT EXISTS dcgm_metrics (
    metric_date date,
    window_start timestamp,
    window_end timestamp,
    name text,
    labels text,
    avg_value double,
    max_value double,
    min_value double,
    PRIMARY KEY ((metric_date, name), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

CREATE TABLE IF NOT EXISTS slurm_metrics (
    metric_date date,
    window_start timestamp,
    window_end timestamp,
    name text,
    labels text,
    avg_value double,
    max_value double,
    min_value double,
    PRIMARY KEY ((metric_date, name), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

-- Create indexes for efficient querying
CREATE INDEX IF NOT EXISTS ipmi_metrics_name_idx ON ipmi_metrics(name);
CREATE INDEX IF NOT EXISTS node_metrics_name_idx ON node_metrics(name);
CREATE INDEX IF NOT EXISTS dcgm_metrics_name_idx ON dcgm_metrics(name);
CREATE INDEX IF NOT EXISTS slurm_metrics_name_idx ON slurm_metrics(name);
