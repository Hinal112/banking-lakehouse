"""
Generate synthetic Canadian retail-banking source data for the medallion pipeline.

Writes four CSV files for batch_1 plus two incremental customer batches to a
Unity Catalog Volume. Data-quality problems are introduced deliberately:

    duplicate customer IDs      -> deduplication + ID-collision handling
    empty vs NULL emails        -> survivorship rules, NULLIF semantics
    inconsistent city casing    -> standardisation
    two date formats            -> multi-format parsing
    orphan account references   -> referential integrity checks
    negative / zero amounts     -> data quality gate

Run inside a Databricks notebook cell, or adapt RAW for local use.
"""

import csv
import os
import random
from datetime import datetime, timedelta

RAW = "/Volumes/banking/raw/landing"

N_CUSTOMERS = 5_000
N_ACCOUNTS = 7_000
N_TXNS = 2_000_000

random.seed(42)

CITIES = ["Ottawa", "Toronto", "Montreal", "Calgary", "Vancouver", "Halifax"]
PROVINCES = {
    "Ottawa": "ON", "Toronto": "ON", "Montreal": "QC",
    "Calgary": "AB", "Vancouver": "BC", "Halifax": "NS",
}
ACCOUNT_TYPES = ["CHEQUING", "SAVINGS", "CREDIT", "MORTGAGE"]
TXN_TYPES = ["PURCHASE", "WITHDRAWAL", "DEPOSIT", "TRANSFER", "PAYMENT"]
MERCHANTS = [
    "Amazon", "Tim Hortons", "Loblaws", "Shell", "Costco",
    "Canadian Tire", "Uber", "Netflix", "Sobeys", "Home Depot",
]
FIRST = [
    "Jonathan", "Jodi", "James", "Alexis", "Brittany", "Katelyn", "Kimberly",
    "Monica", "Daniel", "Priya", "Ahmed", "Wei", "Sofia", "Liam", "Noah", "Emma",
]
LAST = [
    "Cox", "Miller", "Woods", "Smith", "Osborne", "James", "Ortiz", "Sanchez",
    "Tremblay", "Patel", "Khan", "Chen", "Nguyen", "Brown", "Wilson", "Roy",
]

BASE = datetime(2024, 1, 1)

CUST_FIELDS = ["customer_id", "first_name", "last_name", "email",
               "phone", "city", "province", "updated_at"]
ACCT_FIELDS = ["account_id", "customer_id", "account_type", "status",
               "opened_date", "branch_id", "updated_at"]
TXN_FIELDS = ["txn_id", "account_id", "txn_timestamp", "amount",
              "txn_type", "merchant", "currency"]


def dirty_city(city: str) -> str:
    """Return the city name with inconsistent casing or padding."""
    r = random.random()
    if r < 0.15:
        return city.upper()
    if r < 0.30:
        return city.lower()
    if r < 0.40:
        return f"  {city} "
    return city


def dirty_date(d: datetime) -> str:
    """Write a date in one of two formats; 15% use dd/MM/yyyy."""
    return d.strftime("%d/%m/%Y") if random.random() < 0.15 else d.strftime("%Y-%m-%d")


def make_customer(cid: str, updated_at: datetime, city: str) -> dict:
    first, last = random.choice(FIRST), random.choice(LAST)
    email = f"{first.lower()}.{last.lower()}@example.com"

    r = random.random()
    if r < 0.08:
        email = ""          # empty string, not NULL
    elif r < 0.14:
        email = None        # genuine NULL

    phone = f"{random.randint(200, 999)}-{random.randint(200, 999)}-{random.randint(1000, 9999)}"
    if random.random() < 0.10:
        phone = None

    return {
        "customer_id": cid,
        "first_name": first,
        "last_name": last,
        "email": email,
        "phone": phone,
        "city": dirty_city(city),
        "province": PROVINCES[city],
        "updated_at": updated_at.strftime("%Y-%m-%d %H:%M:%S"),
    }


def write_csv(path: str, rows: list, fields: list) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: ("" if v is None else v) for k, v in row.items()})


def main() -> None:
    # ---------------------------------------------------------------- branches
    branches, branch_ids = [], []
    for i, city in enumerate(CITIES + CITIES, start=1):
        bid = f"BR{i:03d}"
        suffix = "Main" if i <= 6 else "West"
        branches.append({
            "branch_id": bid,
            "branch_name": f"{city} {suffix}",
            "city": dirty_city(city),
            "province": PROVINCES[city],
        })
        branch_ids.append(bid)

    write_csv(f"{RAW}/batch_1/branches.csv", branches,
              ["branch_id", "branch_name", "city", "province"])

    # ------------------------------------------------------ customers, batch 1
    customer_city, customers = {}, []
    for i in range(1, N_CUSTOMERS + 1):
        cid = f"C{i:06d}"
        city = random.choice(CITIES)
        customer_city[cid] = city
        updated = BASE + timedelta(days=random.randint(0, 30),
                                   hours=random.randint(0, 23))
        customers.append(make_customer(cid, updated, city))

    # inject ID collisions: 5% of IDs get a second row with a later timestamp
    for dup in random.sample(customers, int(N_CUSTOMERS * 0.05)):
        later = (datetime.strptime(dup["updated_at"], "%Y-%m-%d %H:%M:%S")
                 + timedelta(hours=random.randint(1, 8)))
        customers.append(make_customer(dup["customer_id"], later,
                                       customer_city[dup["customer_id"]]))

    random.shuffle(customers)
    write_csv(f"{RAW}/batch_1/customers.csv", customers, CUST_FIELDS)

    # ------------------------------------------------------- accounts, batch 1
    accounts, account_ids = [], []
    for i in range(1, N_ACCOUNTS + 1):
        aid = f"A{i:07d}"
        accounts.append({
            "account_id": aid,
            "customer_id": f"C{random.randint(1, N_CUSTOMERS):06d}",
            "account_type": random.choice(ACCOUNT_TYPES),
            "status": random.choices(["ACTIVE", "CLOSED", "DORMANT"],
                                     weights=[80, 10, 10])[0],
            "opened_date": dirty_date(BASE - timedelta(days=random.randint(30, 2000))),
            "branch_id": random.choice(branch_ids),
            "updated_at": (BASE + timedelta(days=random.randint(0, 30))
                           ).strftime("%Y-%m-%d %H:%M:%S"),
        })
        account_ids.append(aid)

    write_csv(f"{RAW}/batch_1/accounts.csv", accounts, ACCT_FIELDS)

    # --------------------------------------------------- transactions, batch 1
    txns = []
    for i in range(1, N_TXNS + 1):
        # 0.5% reference an account that does not exist
        if random.random() < 0.005:
            aid = f"A9{random.randint(100000, 999999)}"
        else:
            aid = random.choice(account_ids)

        ts = BASE + timedelta(days=random.randint(0, 89),
                              hours=random.randint(0, 23),
                              minutes=random.randint(0, 59))

        amount = round(random.lognormvariate(3.2, 1.1), 2)
        r = random.random()
        if r < 0.003:
            amount = -amount
        elif r < 0.005:
            amount = 0.0

        txns.append({
            "txn_id": f"T{i:09d}",
            "account_id": aid,
            "txn_timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "amount": amount,
            "txn_type": random.choice(TXN_TYPES),
            "merchant": random.choice(MERCHANTS) if random.random() > 0.05 else "",
            "currency": "CAD",
        })

    write_csv(f"{RAW}/batch_1/transactions.csv", txns, TXN_FIELDS)

    # --------------------------------------------- incremental customer batches
    def make_batch(day: int, n_movers: int, new_start: int, n_new: int) -> list:
        out, when = [], BASE + timedelta(days=day)

        for cid in random.sample(list(customer_city.keys()), n_movers):
            new_city = random.choice([c for c in CITIES if c != customer_city[cid]])
            customer_city[cid] = new_city
            out.append(make_customer(cid, when + timedelta(hours=random.randint(0, 23)),
                                     new_city))

        for i in range(new_start, new_start + n_new):
            cid = f"C{i:06d}"
            city = random.choice(CITIES)
            customer_city[cid] = city
            out.append(make_customer(cid, when + timedelta(hours=random.randint(0, 23)),
                                     city))

        random.shuffle(out)
        return out

    write_csv(f"{RAW}/batch_2/customers.csv",
              make_batch(45, 500, N_CUSTOMERS + 1, 200), CUST_FIELDS)
    write_csv(f"{RAW}/batch_3/customers.csv",
              make_batch(75, 300, N_CUSTOMERS + 201, 150), CUST_FIELDS)

    print("generated")


if __name__ == "__main__":
    main()
