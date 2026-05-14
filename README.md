# BigDataSpark

Лабораторная работа N2 по анализу больших данных: ETL-пайплайн на Apache Spark с загрузкой исходных CSV в PostgreSQL, построением модели звезда и созданием аналитических витрин в ClickHouse.

Исходное техническое задание перенесено в [tz.md](/Users/dkutugin3/Desktop/study/bigdata/BigDataSpark/tz.md).

## Что Сделано

Реализована обязательная часть лабораторной работы:

- поднятие PostgreSQL, Spark и ClickHouse через Docker Compose;
- импорт 10 CSV-файлов из каталога `исходные данные/` в PostgreSQL;
- создание сырого слоя `raw.mock_data` в PostgreSQL;
- построение модели звезда в PostgreSQL в схеме `dwh`;
- создание 6 отчетных таблиц в ClickHouse;
- добавлены команды запуска через `Makefile`;
- добавлены SQL-запросы для проверки результата.

## Архитектура

Пайплайн состоит из трех этапов:

1. PostgreSQL инициализируется скриптом [postgres/init/01_init.sql](/Users/dkutugin3/Desktop/study/bigdata/BigDataSpark/postgres/init/01_init.sql), который создает `raw.mock_data` и загружает все CSV.
2. Spark job [scripts/spark_etl.py](/Users/dkutugin3/Desktop/study/bigdata/BigDataSpark/scripts/spark_etl.py) читает `raw.mock_data`, приводит типы данных и строит модель звезда в PostgreSQL.
3. Этот же Spark job считает агрегированные витрины и записывает их в ClickHouse.

## Структура

```text
.
├── docker-compose.yml
├── Makefile
├── README.md
├── tz.md
├── postgres/
│   └── init/
│       └── 01_init.sql
├── scripts/
│   ├── run_spark_jobs.sh
│   └── spark_etl.py
└── исходные данные/
    ├── MOCK_DATA.csv
    ├── MOCK_DATA (1).csv
    └── ...
```

## Запуск

Требования: Docker и Docker Compose v2.

Полный запуск с очисткой старых volume:

```bash
make reset
make etl
```

Повторный запуск ETL без удаления данных:

```bash
make etl
```

Команды без `make`:

```bash
docker compose down -v
docker compose up -d postgres clickhouse spark
docker compose exec spark /app/scripts/run_spark_jobs.sh
```

## PostgreSQL

Сырой слой:

- `raw.mock_data`

Модель звезда:

- `dwh.dim_customer`
- `dwh.dim_seller`
- `dwh.dim_product`
- `dwh.dim_store`
- `dwh.dim_supplier`
- `dwh.dim_date`
- `dwh.fact_sales`

## ClickHouse

Созданы 6 витрин:

- `sales_by_products`: продажи по продуктам;
- `sales_by_customers`: продажи по клиентам;
- `sales_by_time`: продажи по месяцам и годам;
- `sales_by_stores`: продажи по магазинам;
- `sales_by_suppliers`: продажи по поставщикам;
- `product_quality`: качество продукции, рейтинги и отзывы.



```sql
SELECT count() FROM sales_by_products;

SELECT product_name, product_category, total_quantity_sold, total_revenue
FROM sales_by_products
ORDER BY total_quantity_sold DESC
LIMIT 10;

SELECT customer_first_name, customer_last_name, total_purchase_amount
FROM sales_by_customers
ORDER BY total_purchase_amount DESC
LIMIT 10;

SELECT period_start, total_revenue, avg_order_amount
FROM sales_by_time
ORDER BY period_start;

SELECT store_name, total_revenue
FROM sales_by_stores
ORDER BY total_revenue DESC
LIMIT 5;

SELECT supplier_name, total_revenue, avg_product_price
FROM sales_by_suppliers
ORDER BY total_revenue DESC
LIMIT 5;

SELECT product_name, avg_rating, reviews_count, rating_sales_correlation
FROM product_quality
ORDER BY avg_rating DESC
LIMIT 10;
```
