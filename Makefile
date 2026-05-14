.PHONY: up down reset etl ps logs psql clickhouse

COMPOSE ?= docker compose
SPARK_PACKAGES = org.postgresql:postgresql:42.7.4,com.clickhouse:clickhouse-jdbc:0.6.3,org.apache.httpcomponents.client5:httpclient5:5.2.1

up:
	$(COMPOSE) up -d postgres clickhouse spark

etl: up
	$(COMPOSE) exec spark /opt/spark/bin/spark-submit --packages $(SPARK_PACKAGES) /app/scripts/spark_etl.py

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f postgres clickhouse spark

psql:
	$(COMPOSE) exec postgres psql -U bigdata -d bigdata

clickhouse:
	$(COMPOSE) exec clickhouse clickhouse-client --database bigdata --user bigdata --password bigdata

down:
	$(COMPOSE) down

reset:
	$(COMPOSE) down -v
