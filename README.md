# BigDataFlink

Лабораторная работа №3 по анализу больших данных: streaming processing с Apache Flink.

Проект читает CSV-файлы из каталога `исходные данные`, отправляет каждую CSV-строку в Kafka в формате JSON, затем Flink-job читает Kafka-топик, преобразует поток в модель данных "звезда" и записывает результат в PostgreSQL.

## Что реализовано

- `docker-compose.yml` поднимает PostgreSQL, Kafka, Flink JobManager/TaskManager и producer-сервис.
- `producer/producer.py` читает 10 CSV-файлов, корректно обрабатывает многострочные quoted-поля и публикует JSON-сообщения в Kafka topic `pet-sales`.
- `src/main/java/ru/bigdata/flink/PetSalesStreamingJob.java` читает Kafka stream и пишет star schema в PostgreSQL.
- `postgres/init/001_star_schema.sql` создает таблицы модели "звезда".

## Модель данных

PostgreSQL содержит таблицы:

- `dim_customers`
- `dim_sellers`
- `dim_products`
- `dim_stores`
- `dim_suppliers`
- `dim_dates`
- `fact_sales`

В исходных файлах идентификаторы `id`, `sale_customer_id`, `sale_seller_id`, `sale_product_id` повторяются в разных CSV-файлах, поэтому producer добавляет технические поля `_source_file` и `_source_row_number`. Они используются для стабильного ключа события продажи и source-scoped ключей измерений.

Запись в PostgreSQL выполняется через `INSERT ... ON CONFLICT DO UPDATE`, поэтому повторный запуск producer/job не создает дубликаты по уже обработанным событиям.

## Требования

- Docker и Docker Compose.
- Локально установленный Java/Maven не нужен: jar можно собрать через Maven Docker image.

## Запуск

Собрать Flink jar:

```bash
docker run --rm \
  -v "$PWD":/workspace \
  -w /workspace \
  -v "$PWD/.m2":/root/.m2 \
  maven:3.9.9-eclipse-temurin-17 \
  mvn -DskipTests package
```

Поднять PostgreSQL, Kafka и Flink:

```bash
docker compose up -d postgres kafka jobmanager taskmanager
```

Создать Kafka-топик, если он еще не создан:

```bash
docker compose exec kafka \
  /opt/bitnami/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create \
  --if-not-exists \
  --topic pet-sales \
  --partitions 1 \
  --replication-factor 1
```

Запустить Flink-job:

```bash
docker compose exec jobmanager \
  flink run -d \
  -c ru.bigdata.flink.PetSalesStreamingJob \
  /opt/flink/usrlib/bigdata-flink-lab-1.0.0.jar
```

Отправить CSV-данные в Kafka:

```bash
docker compose build producer
docker compose run --rm producer
```

Flink UI будет доступен по адресу:

```text
http://localhost:8081
```

## Проверка результата

Проверить количество фактов:

```bash
docker compose exec postgres \
  psql -U lab -d pet_sales \
  -c "SELECT COUNT(*) AS fact_rows FROM fact_sales;"
```

Ожидаемый результат после обработки всех файлов:

```text
 fact_rows
-----------
     10000
```

Проверить все таблицы:

```bash
docker compose exec postgres psql -U lab -d pet_sales -c "
SELECT 'dim_customers' AS table_name, COUNT(*) FROM dim_customers
UNION ALL SELECT 'dim_sellers', COUNT(*) FROM dim_sellers
UNION ALL SELECT 'dim_products', COUNT(*) FROM dim_products
UNION ALL SELECT 'dim_stores', COUNT(*) FROM dim_stores
UNION ALL SELECT 'dim_suppliers', COUNT(*) FROM dim_suppliers
UNION ALL SELECT 'dim_dates', COUNT(*) FROM dim_dates
UNION ALL SELECT 'fact_sales', COUNT(*) FROM fact_sales
ORDER BY table_name;"
```

Посмотреть несколько фактов вместе с измерениями:

```bash
docker compose exec postgres psql -U lab -d pet_sales -c "
SELECT
  f.sale_event_id,
  d.full_date,
  c.first_name || ' ' || c.last_name AS customer,
  p.name AS product,
  f.sale_quantity,
  f.sale_total_price
FROM fact_sales f
JOIN dim_dates d ON d.date_key = f.sale_date_key
JOIN dim_customers c ON c.customer_key = f.customer_key
JOIN dim_products p ON p.product_key = f.product_key
LIMIT 10;"
```

## Остановка и очистка

Остановить контейнеры:

```bash
docker compose down
```

Полностью удалить контейнеры и данные PostgreSQL:

```bash
docker compose down -v
```
