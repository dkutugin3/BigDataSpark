#!/usr/bin/env bash
set -euo pipefail

/opt/spark/bin/spark-submit \
  --packages org.postgresql:postgresql:42.7.4,com.clickhouse:clickhouse-jdbc:0.6.3,org.apache.httpcomponents.client5:httpclient5:5.2.1 \
  /app/scripts/spark_etl.py
