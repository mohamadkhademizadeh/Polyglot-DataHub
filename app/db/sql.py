import os, psycopg2
PG_DSN = os.getenv("PG_DSN", "dbname=polyglot user=app password=app host=localhost port=5432")
def get_conn():
    return psycopg2.connect(PG_DSN)
