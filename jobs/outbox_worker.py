import os, time, json, psycopg2
from app.db import dynamo

PG_DSN = os.getenv("PG_DSN", "dbname=polyglot user=app password=app host=localhost port=5432")

def run_batch(cur, size=200):
    cur.execute("SELECT id, payload FROM outbox WHERE processed=false ORDER BY id ASC LIMIT %s", (size,))
    rows = cur.fetchall()
    if not rows: return 0
    items = []
    ids = []
    for oid, payload in rows:
        data = payload if isinstance(payload, dict) else json.loads(payload)
        items.append({"sku": data["sku"], "qty": int(data["qty"])})
        ids.append(oid)
    if items:
        dynamo.reserve(items)
        cur.execute("UPDATE outbox SET processed=true WHERE id = ANY(%s)", (ids,))
    return len(rows)

if __name__ == "__main__":
    print("[outbox] worker watching... Ctrl+C to stop")
    while True:
        with psycopg2.connect(PG_DSN) as conn:
            with conn.cursor() as cur:
                n = run_batch(cur)
            conn.commit()
        if n == 0:
            time.sleep(1.0)
