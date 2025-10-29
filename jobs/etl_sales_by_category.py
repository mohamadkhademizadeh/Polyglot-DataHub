import os, psycopg2, pymongo, pandas as pd

PG_DSN = os.getenv("PG_DSN", "dbname=polyglot user=app password=app host=localhost port=5432")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")

def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute("""CREATE TABLE IF NOT EXISTS sales_by_category(
            category TEXT PRIMARY KEY,
            orders INT NOT NULL,
            qty INT NOT NULL,
            revenue_cents BIGINT NOT NULL
        )""")
    conn.commit()

def run_etl():
    mc = pymongo.MongoClient(MONGO_URI)
    sku2cat = {d["sku"]: d.get("category","unknown") for d in mc["polyglot"]["products"].find({}, {"sku":1,"category":1})}

    with psycopg2.connect(PG_DSN) as conn:
        ensure_table(conn)
        q = """SELECT o.id as order_id, oi.sku, oi.qty, oi.unit_price_cents
                FROM order_items oi JOIN orders o ON oi.order_id=o.id"""
        df = pd.read_sql(q, conn)
        if df.empty:
            print("[etl] no data")
            return
        df["category"] = df["sku"].map(lambda s: sku2cat.get(s,"unknown"))
        agg = df.groupby("category").agg(orders=("order_id","nunique"),
                                         qty=("qty","sum"),
                                         revenue_cents=("unit_price_cents","sum")).reset_index()
        with conn.cursor() as cur:
            cur.execute("TRUNCATE sales_by_category")
            for _,r in agg.iterrows():
                cur.execute("INSERT INTO sales_by_category(category,orders,qty,revenue_cents) VALUES (%s,%s,%s,%s)",
                            (r["category"], int(r["orders"]), int(r["qty"]), int(r["revenue_cents"])))
        conn.commit()
        print("[etl] sales_by_category refreshed")

if __name__ == "__main__":
    run_etl()
