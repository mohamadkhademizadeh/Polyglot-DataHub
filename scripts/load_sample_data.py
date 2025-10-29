import os, random, time
import psycopg2, pymongo, boto3
from faker import Faker
from tqdm import trange

PG_DSN = os.getenv("PG_DSN", "dbname=polyglot user=app password=app host=localhost port=5432")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DDB_ENDPOINT = os.getenv("DDB_ENDPOINT", "http://localhost:8000")
fake = Faker()

def seed_customers(n=50):
    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            for _ in trange(n, desc="customers"):
                name = fake.name()
                email = f"{name.split()[0].lower()}{random.randint(1,9999)}@example.com"
                cur.execute("INSERT INTO customers(email,name) VALUES (%s,%s) ON CONFLICT (email) DO NOTHING", (email, name))
        conn.commit()

def seed_products(n=120):
    cats = ["fasteners","pcb","sensor","motor","tool","bearing","cable"]
    mc = pymongo.MongoClient(MONGO_URI)
    coll = mc["polyglot"]["products"]
    for i in trange(n, desc="products"):
        sku = f"SKU-{i:03d}"
        doc = {
            "sku": sku,
            "title": fake.sentence(nb_words=3),
            "category": random.choice(cats),
            "attributes": {"material": random.choice(["steel","al","cu","abs"]),
                           "color": random.choice(["black","silver","red","blue"])},
            "price_cents": random.randint(300, 25000),
            "tags": random.sample(["new","sale","premium","eco","clearance"], k=random.randint(1,3))
        }
        coll.update_one({"sku": sku}, {"$set": doc}, upsert=True)

def seed_inventory():
    ddb = boto3.resource("dynamodb", endpoint_url=DDB_ENDPOINT, region_name="us-east-1")
    tab = ddb.Table("inventory_events")
    now = int(time.time()*1000)
    for i in trange(120, desc="inventory"):
        sku = f"SKU-{i:03d}"
        qty = random.randint(20, 200)
        tab.put_item(Item={"sku": sku, "ts": now, "type": "STOCK_SET", "qty": qty})

def seed_orders(n=400):
    mc = pymongo.MongoClient(MONGO_URI)
    products = list(mc["polyglot"]["products"].find({}, {"sku":1,"price_cents":1}))
    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id,email FROM customers")
            custs = cur.fetchall()
            for _ in trange(n, desc="orders"):
                customer_id, _email = random.choice(custs)
                items = random.sample(products, k=random.randint(1,3))
                total = 0
                # create order
                for it in items:
                    total += it["price_cents"]*random.randint(1,3)
                cur.execute("INSERT INTO orders(customer_id,total_cents,currency) VALUES (%s,%s,%s) RETURNING id",
                            (customer_id, total, "USD"))
                order_id = cur.fetchone()[0]
                # items
                for it in items:
                    qty = random.randint(1,3)
                    cur.execute("INSERT INTO order_items(order_id,sku,qty,unit_price_cents) VALUES (%s,%s,%s,%s)",
                                (order_id, it["sku"], qty, it["price_cents"]))
        conn.commit()

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--customers", type=int, default=50)
    ap.add_argument("--products", type=int, default=120)
    ap.add_argument("--orders", type=int, default=400)
    args = ap.parse_args()
    seed_customers(args.customers)
    seed_products(args.products)
    seed_inventory()
    seed_orders(args.orders)
    print("Seeded data.")
