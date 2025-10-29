# Polyglot-DataHub (Full)
**PostgreSQL + MongoDB + DynamoDB** polyglot persistence demo with:
- **Postgres:** customers, orders, order_items, outbox (CDC)
- **MongoDB:** product catalog
- **DynamoDB:** inventory event stream (reservations)
- **FastAPI:** unified service (health, products, orders)
- **Outbox worker:** reads Postgres → writes Dynamo reservations
- **ETL:** joins SQL orders + Mongo categories → sales_by_category table

## Quickstart
```bash
# 0) prereqs: docker, docker compose, Python 3.10+

# 1) Start infra
docker compose up -d

# 2) Install deps
pip install -r requirements.txt

# 3) Init databases (DDL, indexes, Dynamo table)
python scripts/init_db.py

# 4) Load sample data
python scripts/load_sample_data.py --customers 50 --products 120 --orders 400

# 5) Run API
uvicorn app.main:app --host 0.0.0.0 --port 8000

# 6) In another shell, run CDC outbox worker
python jobs/outbox_worker.py

# 7) Run nightly ETL to build analytics
python jobs/etl_sales_by_category.py
```

## Endpoints
- `GET /health` – basic liveness
- `GET /products/{sku}` – product from Mongo
- `POST /products` – create/update product (Mongo)
- `POST /customers` – create customer (Postgres)
- `POST /orders` – create order (Postgres), prices from Mongo; outbox rows emitted by trigger
- `GET /analytics/sales_by_category` – read aggregated table (Postgres)

## Environment (defaults work with docker compose)
- `PG_DSN=dbname=polyglot user=app password=app host=localhost port=5432`
- `MONGO_URI=mongodb://localhost:27017`
- `DDB_ENDPOINT=http://localhost:8000`
