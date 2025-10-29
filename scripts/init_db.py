import os, time, psycopg2, pymongo, boto3

PG_DSN = os.getenv("PG_DSN", "dbname=polyglot user=app password=app host=localhost port=5432")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DDB_ENDPOINT = os.getenv("DDB_ENDPOINT", "http://localhost:8000")

print("[init] PostgreSQL schema ...")
with psycopg2.connect(PG_DSN) as conn:
    with conn.cursor() as cur:
        cur.execute(open("scripts/init_postgres.sql").read())
    conn.commit()

print("[init] MongoDB indexes ...")
mc = pymongo.MongoClient(MONGO_URI)
products = mc["polyglot"]["products"]
products.create_index("sku", unique=True)
products.create_index([("category", 1)])

print("[init] DynamoDB table ...")
ddb = boto3.resource("dynamodb", endpoint_url=DDB_ENDPOINT, region_name="us-east-1")
names = [t.name for t in ddb.tables.all()]
if "inventory_events" not in names:
    ddb.create_table(
        TableName="inventory_events",
        KeySchema=[{"AttributeName":"sku","KeyType":"HASH"},
                   {"AttributeName":"ts","KeyType":"RANGE"}],
        AttributeDefinitions=[{"AttributeName":"sku","AttributeType":"S"},
                              {"AttributeName":"ts","AttributeType":"N"}],
        BillingMode="PAY_PER_REQUEST"
    )
    # wait for active
    while True:
        status = ddb.Table("inventory_events").table_status
        if status == "ACTIVE":
            break
        time.sleep(0.5)

print("[init] done.")
