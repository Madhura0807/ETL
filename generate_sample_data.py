"""Generate SYNTHETIC sample data (nothing here is real). Deterministic (seeded).

Writes:
  data/raw/customers.csv   - CSV with deliberate defects (duplicates, bad emails, nulls, whitespace,
                             malformed row, out-of-range ages, mixed-case values)
  data/raw/orders.json     - nested JSON, some orders reference unknown customers / bad quantities
  data/raw/source.db       - SQLite 'source database' with a products table
Optional schema-drift variants of the customers file:  --drift added|removed|type_change
"""
import argparse
import csv
import json
import random
import sqlite3
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
random.seed(42)

FIRST = ["Aarav", "Meera", "Rohan", "Isha", "Kabir", "Ananya", "Vivaan", "Diya", "Arjun", "Sara", "Liam", "Emma"]
LAST = ["Sharma", "Patil", "Mehta", "Iyer", "Khan", "Nair", "Singh", "Gupta", "Smith", "Brown"]
COUNTRIES = ["in", "us", "gb", "de", "IN", "US"]
TIERS = ["standard", "silver", "gold", "", None]


def customers(n=120):
    rows = []
    for i in range(1, n + 1):
        name = f"{random.choice(FIRST)} {random.choice(LAST)}"
        rows.append({
            "Customer ID": i,
            "Customer Name": f"  {name}  " if i % 15 == 0 else name,
            "Email": f"{name.lower().replace(' ', '.')}{i}@example.com",
            "Phone": f"+91-98{random.randint(10000000, 99999999)}" if random.random() > 0.15 else "",
            "Age": random.randint(18, 75) if random.random() > 0.10 else "",
            "Country": random.choice(COUNTRIES),
            "Signup Date": (date(2019, 1, 1) + timedelta(days=random.randint(0, 1700))).isoformat()
            if i % 7 else (date(2021, 1, 1) + timedelta(days=i)).strftime("%d/%m/%Y"),
            "Loyalty Tier": random.choice(TIERS) or "",
        })
    rows[9]["Email"] = "not-an-email"            # invalid email
    rows[19]["Email"] = "missing-at-sign.example.com"
    rows[29]["Age"] = 17                          # below range
    rows[39]["Age"] = 140                         # above range
    rows[49]["Customer Name"] = ""                # required value missing
    rows[59]["Signup Date"] = "2099-01-01"        # future date
    rows[69]["Signup Date"] = "31-31-2020"        # not a date
    rows.append(dict(rows[4]))                    # exact duplicate row
    dup_key = dict(rows[14])
    dup_key["Email"] = "different.email@example.com"
    rows.append(dup_key)                          # same primary key, different data
    return rows


def write_customers(path, rows, drift=None):
    fields = list(rows[0].keys())
    if drift == "added":
        fields.append("Marketing Opt In")
        for r in rows:
            r["Marketing Opt In"] = random.choice(["true", "false"])
    elif drift == "removed":
        fields.remove("Phone")
    elif drift == "type_change":
        for r in rows:
            r["Customer ID"] = f"CUST-{int(r['Customer ID']):04d}"  # integer ids become strings
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
        if drift is None:
            f.write("999,Broken Row,broken@example.com,1,2,3,4,5,6,7,8\n")  # malformed: too many fields


def orders(n=300, customer_count=120):
    out = []
    for i in range(1, n + 1):
        out.append({
            "order_id": 1000 + i,
            "customer_id": random.randint(1, customer_count),
            "order_date": (date(2024, 1, 1) + timedelta(days=i // 2)).isoformat(),
            "status": random.choice(["Shipped", "PENDING", "delivered", " Cancelled "]),
            "quantity": random.randint(1, 10),
            "unit_price": round(random.uniform(5, 500), 2),
            "shipping": {"city": random.choice(["Pune", "Mumbai", "Berlin", "Austin"]),
                         "country": random.choice(["IN", "DE", "US"])},
            "payment": {"method": random.choice(["card", "upi", "cod"])},
        })
    out[10]["customer_id"] = 9999        # unknown customer (referential integrity)
    out[20]["quantity"] = 0              # out of range
    out[30]["unit_price"] = -5.0         # negative price
    del out[40]["quantity"]              # missing field
    out[50]["order_date"] = "not a date"
    out.append(dict(out[5]))             # duplicate order
    return {"orders": out}


def products(db_path, n=90):
    if db_path.exists():
        db_path.unlink()
    con = sqlite3.connect(db_path)
    con.execute("""CREATE TABLE products (product_id INTEGER PRIMARY KEY, product_name TEXT, category TEXT,
                   price REAL, stock_qty INTEGER, supplier_email TEXT, last_updated TEXT, is_active INTEGER)""")
    cats = ["electronics", "home", "books", "toys", "garden"]
    for i in range(1, n + 1):
        con.execute("INSERT INTO products VALUES (?,?,?,?,?,?,?,?)", (
            i, f"Product {i}", random.choice(cats), round(random.uniform(3, 900), 2),
            random.randint(0, 500) if i % 9 else None, f"Sales@Supplier{i % 7}.com",
            (date(2024, 1, 1) + timedelta(days=i)).isoformat(), 1 if i % 10 else 0))
    con.execute("UPDATE products SET price = -1 WHERE product_id = 11")           # invalid price
    con.execute("UPDATE products SET supplier_email = 'bad-email' WHERE product_id = 22")
    con.commit()
    con.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--drift", choices=["added", "removed", "type_change"])
    args = ap.parse_args()
    RAW.mkdir(parents=True, exist_ok=True)
    rows = customers()
    if args.drift:
        target = RAW / f"customers_drift_{args.drift}.csv"
        write_customers(target, rows, args.drift)
        print(f"wrote {target}")
    else:
        write_customers(RAW / "customers.csv", rows)
        (RAW / "orders.json").write_text(json.dumps(orders(), indent=2), encoding="utf-8")
        products(RAW / "source.db")
        print("wrote customers.csv, orders.json, source.db (synthetic)")
