from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, EmailStr
from typing import List
from app.db import sql, mongo, dynamo

app = FastAPI(title="Polyglot DataHub (Full)")

class Product(BaseModel):
    sku: str
    title: str
    category: str
    attributes: dict = Field(default_factory=dict)
    price_cents: int
    tags: List[str] = Field(default_factory=list)

class Item(BaseModel):
    sku: str
    qty: int = Field(gt=0)

class OrderIn(BaseModel):
    customer_email: EmailStr
    items: List[Item]

class CustomerIn(BaseModel):
    email: EmailStr
    name: str

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/products")
def upsert_product(body: Product):
    mongo.products.update_one({"sku": body.sku}, {"$set": body.model_dump()}, upsert=True)
    return {"ok": True}

@app.get("/products/{sku}")
def get_product(sku: str):
    doc = mongo.products.find_one({"sku": sku}, {"_id": 0})
    if not doc: raise HTTPException(404, "not found")
    return doc

@app.post("/customers")
def create_customer(body: CustomerIn):
    with sql.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO customers(email,name) VALUES (%s,%s) ON CONFLICT (email) DO NOTHING", (body.email, body.name))
        conn.commit()
    return {"ok": True}

@app.post("/orders")
def create_order(body: OrderIn):
    # resolve prices in Mongo
    skus = [i.sku for i in body.items]
    price_map = {d["sku"]: d["price_cents"] for d in mongo.products.find({"sku": {"$in": skus}}, {"sku":1,"price_cents":1})}
    if any(s not in price_map for s in skus):
        raise HTTPException(400, "unknown sku")
    with sql.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM customers WHERE email=%s", (body.customer_email,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(400, "unknown customer")
            cust_id = row[0]
            total = sum(price_map[i.sku] * i.qty for i in body.items)
            cur.execute("INSERT INTO orders(customer_id,total_cents,currency) VALUES (%s,%s,%s) RETURNING id",
                        (cust_id, total, "USD"))
            oid = cur.fetchone()[0]
            for i in body.items:
                cur.execute("INSERT INTO order_items(order_id,sku,qty,unit_price_cents) VALUES (%s,%s,%s,%s)",
                            (oid, i.sku, i.qty, price_map[i.sku]))
        conn.commit()
    # outbox trigger on order_items will create events; worker will reserve in Dynamo
    return {"order_id": oid, "total_cents": total}

@app.get("/analytics/sales_by_category")
def sales_by_category():
    with sql.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT category, orders, qty, revenue_cents FROM sales_by_category ORDER BY revenue_cents DESC")
            rows = cur.fetchall()
            return [{"category": r[0], "orders": r[1], "qty": r[2], "revenue_cents": r[3]} for r in rows]
