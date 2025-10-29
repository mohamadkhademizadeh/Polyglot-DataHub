import os, boto3, time
DDB_ENDPOINT = os.getenv("DDB_ENDPOINT", "http://localhost:8000")
ddb = boto3.resource("dynamodb", endpoint_url=DDB_ENDPOINT, region_name="us-east-1")
inventory = ddb.Table("inventory_events")

def reserve(items):
    ts = int(time.time()*1000)
    with inventory.batch_writer() as bw:
        for it in items:
            bw.put_item(Item={"sku": it["sku"], "ts": ts, "type": "RESERVE", "qty": int(it["qty"])})
