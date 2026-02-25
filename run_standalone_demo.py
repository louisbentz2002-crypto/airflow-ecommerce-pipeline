"""
Standalone Demo - No External Dependencies Required

This script demonstrates the data pipeline using only
Python standard library + pandas (usually pre-installed).

Usage:
    python run_standalone_demo.py
"""

import os
import sys
import sqlite3
import random
import string
from datetime import date, datetime, timedelta
from pathlib import Path

# Check for pandas
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False
    print("Note: pandas not installed, using CSV module instead")


# ============================================
# SIMPLE DATA GENERATOR (no faker needed)
# ============================================

class SimpleDataGenerator:
    """Generate synthetic data without external dependencies."""

    FIRST_NAMES = [
        "Emma", "Liam", "Olivia", "Noah", "Ava", "Ethan", "Sophia", "Mason",
        "Isabella", "William", "Mia", "James", "Charlotte", "Oliver", "Amelia",
        "Benjamin", "Harper", "Elijah", "Evelyn", "Lucas", "Marie", "Jean",
        "Pierre", "Claire", "Louis", "Julie", "Thomas", "Camille", "Nicolas"
    ]

    LAST_NAMES = [
        "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
        "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
        "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Martin", "Lee",
        "Dupont", "Durand", "Moreau", "Bernard", "Petit", "Robert", "Richard"
    ]

    CITIES = [
        "Paris", "Lyon", "Marseille", "Toulouse", "Nice", "Nantes", "Strasbourg",
        "Montpellier", "Bordeaux", "Lille", "New York", "Los Angeles", "Chicago",
        "Houston", "Phoenix", "London", "Manchester", "Birmingham", "Glasgow"
    ]

    COUNTRIES = ["FR", "US", "UK", "DE", "ES", "IT", "BE", "NL", "CH"]

    CATEGORIES = {
        "Electronics": ["Smartphone", "Laptop", "Tablet", "Headphones", "Camera"],
        "Clothing": ["T-Shirt", "Jeans", "Jacket", "Shoes", "Hat"],
        "Home": ["Chair", "Table", "Lamp", "Rug", "Mirror"],
        "Sports": ["Ball", "Racket", "Weights", "Mat", "Bike"],
        "Books": ["Novel", "Guide", "Manual", "Comic", "Magazine"],
    }

    SUPPLIERS = [
        "TechSupply Co", "Global Imports", "Quality Goods Inc", "FastShip Ltd",
        "Premium Products", "Value Distributors", "ProSupply", "MegaStore"
    ]

    STATUSES = ["COMPLETED", "SHIPPED", "DELIVERED", "PENDING", "CANCELLED"]
    STATUS_WEIGHTS = [60, 15, 10, 10, 5]

    PAYMENT_METHODS = ["CREDIT_CARD", "DEBIT_CARD", "PAYPAL", "BANK_TRANSFER"]
    PAYMENT_STATUSES = ["COMPLETED", "PENDING", "FAILED"]

    def __init__(self, seed: int = 42):
        self.seed = seed
        random.seed(seed)

    def _random_email(self, first: str, last: str) -> str:
        domains = ["gmail.com", "yahoo.com", "outlook.com", "email.fr", "mail.com"]
        return f"{first.lower()}.{last.lower()}{random.randint(1,99)}@{random.choice(domains)}"

    def _random_phone(self) -> str:
        return f"+{random.randint(1,99)} {random.randint(100,999)} {random.randint(100,999)} {random.randint(1000,9999)}"

    def _random_address(self) -> str:
        return f"{random.randint(1, 200)} {random.choice(['Rue', 'Avenue', 'Boulevard'])} {random.choice(self.LAST_NAMES)}"

    def _weighted_choice(self, choices: list, weights: list) -> str:
        total = sum(weights)
        r = random.randint(1, total)
        cumsum = 0
        for choice, weight in zip(choices, weights):
            cumsum += weight
            if r <= cumsum:
                return choice
        return choices[-1]

    def generate_customers(self, n: int = 100) -> list[dict]:
        customers = []
        for i in range(1, n + 1):
            first = random.choice(self.FIRST_NAMES)
            last = random.choice(self.LAST_NAMES)
            days_ago = random.randint(0, 1000)
            customers.append({
                "customer_id": i,
                "first_name": first,
                "last_name": last,
                "email": self._random_email(first, last),
                "phone": self._random_phone(),
                "address": self._random_address(),
                "city": random.choice(self.CITIES),
                "country": random.choice(self.COUNTRIES),
                "created_at": (date.today() - timedelta(days=days_ago)).isoformat(),
            })
        return customers

    def generate_products(self, n: int = 50) -> list[dict]:
        products = []
        for i in range(1, n + 1):
            category = random.choice(list(self.CATEGORIES.keys()))
            subcategory = random.choice(self.CATEGORIES[category])
            price = round(random.uniform(10, 500), 2)
            cost = round(price * random.uniform(0.4, 0.7), 2)
            products.append({
                "product_id": i,
                "product_name": f"{subcategory} {random.choice(['Pro', 'Plus', 'Basic', 'Premium', 'Lite'])} {random.randint(100, 999)}",
                "category": category,
                "subcategory": subcategory,
                "price": price,
                "cost": cost,
                "stock_quantity": random.randint(0, 500),
                "supplier": random.choice(self.SUPPLIERS),
            })
        return products

    def generate_orders(self, n: int = 500, customer_ids: list = None, product_ids: list = None, order_date: date = None) -> list[dict]:
        if customer_ids is None:
            customer_ids = list(range(1, 101))
        if product_ids is None:
            product_ids = list(range(1, 51))
        if order_date is None:
            order_date = date.today()

        orders = []
        for i in range(1, n + 1):
            qty = random.randint(1, 5)
            price = round(random.uniform(15, 300), 2)
            orders.append({
                "order_id": i,
                "customer_id": random.choice(customer_ids),
                "product_id": random.choice(product_ids),
                "quantity": qty,
                "unit_price": price,
                "total_amount": round(qty * price, 2),
                "order_date": order_date.isoformat(),
                "order_status": self._weighted_choice(self.STATUSES, self.STATUS_WEIGHTS),
                "shipping_address": f"{self._random_address()}, {random.choice(self.CITIES)}",
            })
        return orders

    def generate_payments(self, order_ids: list, payment_date: date = None) -> list[dict]:
        if payment_date is None:
            payment_date = date.today()

        payments = []
        for i, order_id in enumerate(order_ids, 1):
            payments.append({
                "payment_id": i,
                "order_id": order_id,
                "payment_method": random.choice(self.PAYMENT_METHODS),
                "payment_amount": round(random.uniform(20, 800), 2),
                "payment_date": payment_date.isoformat(),
                "payment_status": random.choices(self.PAYMENT_STATUSES, weights=[85, 10, 5])[0],
            })
        return payments


# ============================================
# CSV UTILITIES
# ============================================

def write_csv(data: list[dict], filepath: Path):
    """Write list of dicts to CSV without pandas."""
    import csv
    if not data:
        return
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)


def read_csv(filepath: Path) -> list[dict]:
    """Read CSV file to list of dicts."""
    import csv
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        return list(reader)


# ============================================
# DATABASE FUNCTIONS
# ============================================

def create_database(db_path: str) -> sqlite3.Connection:
    """Create SQLite database with all tables."""
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        -- RAW TABLES
        CREATE TABLE IF NOT EXISTS raw_orders (
            order_id TEXT, customer_id TEXT, product_id TEXT,
            quantity INTEGER, unit_price REAL, total_amount REAL,
            order_date TEXT, order_status TEXT, shipping_address TEXT,
            _loaded_at TEXT, _source_file TEXT, _execution_date TEXT
        );
        CREATE TABLE IF NOT EXISTS raw_customers (
            customer_id TEXT, first_name TEXT, last_name TEXT,
            email TEXT, phone TEXT, address TEXT, city TEXT, country TEXT,
            created_at TEXT, _loaded_at TEXT, _source_file TEXT, _execution_date TEXT
        );
        CREATE TABLE IF NOT EXISTS raw_products (
            product_id TEXT, product_name TEXT, category TEXT, subcategory TEXT,
            price REAL, cost REAL, stock_quantity INTEGER, supplier TEXT,
            _loaded_at TEXT, _source_file TEXT, _execution_date TEXT
        );
        CREATE TABLE IF NOT EXISTS raw_payments (
            payment_id TEXT, order_id TEXT, payment_method TEXT,
            payment_amount REAL, payment_date TEXT, payment_status TEXT,
            _loaded_at TEXT, _source_file TEXT, _execution_date TEXT
        );

        -- STAGING TABLES
        CREATE TABLE IF NOT EXISTS stg_orders (
            order_id INTEGER PRIMARY KEY, customer_id INTEGER, product_id INTEGER,
            quantity INTEGER, unit_price REAL, total_amount REAL,
            order_date TEXT, order_status TEXT, shipping_address TEXT,
            _loaded_at TEXT, _execution_date TEXT
        );
        CREATE TABLE IF NOT EXISTS stg_customers (
            customer_id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT,
            full_name TEXT, email TEXT, phone TEXT, address TEXT, city TEXT,
            country TEXT, created_at TEXT, _loaded_at TEXT, _execution_date TEXT
        );
        CREATE TABLE IF NOT EXISTS stg_products (
            product_id INTEGER PRIMARY KEY, product_name TEXT, category TEXT,
            subcategory TEXT, price REAL, cost REAL, margin REAL, margin_pct REAL,
            stock_quantity INTEGER, supplier TEXT, _loaded_at TEXT, _execution_date TEXT
        );
        CREATE TABLE IF NOT EXISTS stg_payments (
            payment_id INTEGER PRIMARY KEY, order_id INTEGER, payment_method TEXT,
            payment_amount REAL, payment_date TEXT, payment_status TEXT,
            _loaded_at TEXT, _execution_date TEXT
        );

        -- MART TABLES
        CREATE TABLE IF NOT EXISTS mart_revenue_daily (
            order_date TEXT PRIMARY KEY, total_orders INTEGER,
            total_quantity INTEGER, total_revenue REAL,
            average_order_value REAL, _updated_at TEXT
        );
        CREATE TABLE IF NOT EXISTS mart_top_products (
            product_id INTEGER, product_name TEXT, category TEXT,
            total_quantity INTEGER, total_revenue REAL, order_count INTEGER,
            rank INTEGER, period_start TEXT, period_end TEXT, _updated_at TEXT,
            PRIMARY KEY (product_id, period_start)
        );
        CREATE TABLE IF NOT EXISTS mart_customer_ltv (
            customer_id INTEGER PRIMARY KEY, full_name TEXT, email TEXT,
            first_order_date TEXT, last_order_date TEXT, total_orders INTEGER,
            total_quantity INTEGER, lifetime_value REAL, average_order_value REAL,
            customer_tenure_days INTEGER, _updated_at TEXT
        );
    """)
    conn.commit()
    return conn


def load_raw_data(conn: sqlite3.Connection, data: list[dict], table: str, exec_date: str, filename: str):
    """Load data into raw table."""
    if not data:
        return
    now = datetime.utcnow().isoformat()
    conn.execute(f"DELETE FROM {table} WHERE _execution_date = ?", (exec_date,))

    columns = list(data[0].keys()) + ["_loaded_at", "_source_file", "_execution_date"]
    placeholders = ", ".join(["?"] * len(columns))

    for row in data:
        values = list(row.values()) + [now, filename, exec_date]
        conn.execute(f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})", values)
    conn.commit()


# ============================================
# PIPELINE STEPS
# ============================================

def run_pipeline():
    """Run the complete ELT pipeline."""
    print("\n" + "=" * 60)
    print("🚀 AIRFLOW E-COMMERCE PIPELINE - STANDALONE DEMO")
    print("=" * 60)
    print("Running without Docker, Airflow, or external dependencies\n")

    project_dir = Path(__file__).parent
    data_dir = project_dir / "data" / "incoming"
    db_path = project_dir / "demo_standalone.db"
    exec_date = date.today().isoformat()

    # Step 1: Generate data
    print("📊 STEP 1: Generating synthetic data...")
    gen = SimpleDataGenerator(seed=42)

    customers = gen.generate_customers(100)
    products = gen.generate_products(50)
    customer_ids = [c["customer_id"] for c in customers]
    product_ids = [p["product_id"] for p in products]
    orders = gen.generate_orders(500, customer_ids, product_ids)
    order_ids = [o["order_id"] for o in orders]
    payments = gen.generate_payments(order_ids)

    # Save to CSV
    data_dir.mkdir(parents=True, exist_ok=True)
    write_csv(customers, data_dir / f"customers_{exec_date}.csv")
    write_csv(products, data_dir / f"products_{exec_date}.csv")
    write_csv(orders, data_dir / f"orders_{exec_date}.csv")
    write_csv(payments, data_dir / f"payments_{exec_date}.csv")
    print(f"   ✓ Generated: {len(customers)} customers, {len(products)} products")
    print(f"   ✓ Generated: {len(orders)} orders, {len(payments)} payments")

    # Step 2: Create database and load raw
    print("\n📥 STEP 2: Loading into raw layer...")
    conn = create_database(str(db_path))
    load_raw_data(conn, customers, "raw_customers", exec_date, f"customers_{exec_date}.csv")
    load_raw_data(conn, products, "raw_products", exec_date, f"products_{exec_date}.csv")
    load_raw_data(conn, orders, "raw_orders", exec_date, f"orders_{exec_date}.csv")
    load_raw_data(conn, payments, "raw_payments", exec_date, f"payments_{exec_date}.csv")
    print("   ✓ Loaded all data into raw tables")

    # Step 3: Transform to staging
    print("\n🔄 STEP 3: Transforming to staging layer...")
    conn.executescript("""
        DELETE FROM stg_orders;
        INSERT INTO stg_orders SELECT
            CAST(order_id AS INTEGER), CAST(customer_id AS INTEGER),
            CAST(product_id AS INTEGER), quantity, unit_price, total_amount,
            order_date, UPPER(order_status), shipping_address, _loaded_at, _execution_date
        FROM raw_orders WHERE order_id IS NOT NULL GROUP BY CAST(order_id AS INTEGER);

        DELETE FROM stg_customers;
        INSERT INTO stg_customers SELECT
            CAST(customer_id AS INTEGER), first_name, last_name,
            first_name || ' ' || last_name, LOWER(email), phone, address,
            city, UPPER(country), created_at, _loaded_at, _execution_date
        FROM raw_customers WHERE customer_id IS NOT NULL GROUP BY CAST(customer_id AS INTEGER);

        DELETE FROM stg_products;
        INSERT INTO stg_products SELECT
            CAST(product_id AS INTEGER), product_name, category, subcategory,
            price, cost, price - cost,
            CASE WHEN price > 0 THEN ROUND((price - cost) / price * 100, 2) ELSE 0 END,
            stock_quantity, supplier, _loaded_at, _execution_date
        FROM raw_products WHERE product_id IS NOT NULL GROUP BY CAST(product_id AS INTEGER);

        DELETE FROM stg_payments;
        INSERT INTO stg_payments SELECT
            CAST(payment_id AS INTEGER), CAST(order_id AS INTEGER),
            UPPER(payment_method), payment_amount, payment_date,
            UPPER(payment_status), _loaded_at, _execution_date
        FROM raw_payments WHERE payment_id IS NOT NULL GROUP BY CAST(payment_id AS INTEGER);
    """)
    conn.commit()

    counts = {
        "stg_orders": conn.execute("SELECT COUNT(*) FROM stg_orders").fetchone()[0],
        "stg_customers": conn.execute("SELECT COUNT(*) FROM stg_customers").fetchone()[0],
        "stg_products": conn.execute("SELECT COUNT(*) FROM stg_products").fetchone()[0],
        "stg_payments": conn.execute("SELECT COUNT(*) FROM stg_payments").fetchone()[0],
    }
    for table, count in counts.items():
        print(f"   ✓ {table}: {count} rows")

    # Step 4: Build marts
    print("\n📈 STEP 4: Building analytics marts...")
    now = datetime.utcnow().isoformat()

    conn.execute("DELETE FROM mart_revenue_daily")
    conn.execute(f"""
        INSERT INTO mart_revenue_daily
        SELECT order_date, COUNT(DISTINCT order_id), SUM(quantity), SUM(total_amount),
               ROUND(SUM(total_amount) * 1.0 / COUNT(DISTINCT order_id), 2), '{now}'
        FROM stg_orders
        WHERE order_status IN ('COMPLETED', 'DELIVERED', 'SHIPPED')
        GROUP BY order_date
    """)
    print(f"   ✓ mart_revenue_daily: {conn.execute('SELECT COUNT(*) FROM mart_revenue_daily').fetchone()[0]} rows")

    conn.execute("DELETE FROM mart_top_products")
    conn.execute(f"""
        INSERT INTO mart_top_products
        SELECT o.product_id, p.product_name, p.category, SUM(o.quantity), SUM(o.total_amount),
               COUNT(DISTINCT o.order_id), ROW_NUMBER() OVER (ORDER BY SUM(o.total_amount) DESC),
               DATE('now', '-7 days'), DATE('now'), '{now}'
        FROM stg_orders o JOIN stg_products p ON o.product_id = p.product_id
        WHERE o.order_status IN ('COMPLETED', 'DELIVERED', 'SHIPPED')
        GROUP BY o.product_id, p.product_name, p.category
        ORDER BY SUM(o.total_amount) DESC LIMIT 20
    """)
    print(f"   ✓ mart_top_products: {conn.execute('SELECT COUNT(*) FROM mart_top_products').fetchone()[0]} rows")

    conn.execute("DELETE FROM mart_customer_ltv")
    conn.execute(f"""
        INSERT INTO mart_customer_ltv
        SELECT o.customer_id, c.full_name, c.email, MIN(o.order_date), MAX(o.order_date),
               COUNT(DISTINCT o.order_id), SUM(o.quantity), SUM(o.total_amount),
               ROUND(SUM(o.total_amount) * 1.0 / COUNT(DISTINCT o.order_id), 2),
               CAST(JULIANDAY(MAX(o.order_date)) - JULIANDAY(MIN(o.order_date)) AS INTEGER), '{now}'
        FROM stg_orders o JOIN stg_customers c ON o.customer_id = c.customer_id
        WHERE o.order_status IN ('COMPLETED', 'DELIVERED', 'SHIPPED')
        GROUP BY o.customer_id, c.full_name, c.email
    """)
    print(f"   ✓ mart_customer_ltv: {conn.execute('SELECT COUNT(*) FROM mart_customer_ltv').fetchone()[0]} rows")
    conn.commit()

    # Step 5: Show results
    print("\n" + "=" * 60)
    print("📊 RESULTS")
    print("=" * 60)

    print("\n🏆 Top 5 Products by Revenue:")
    print("-" * 50)
    rows = conn.execute("""
        SELECT rank, product_name, category, printf('$%.2f', total_revenue)
        FROM mart_top_products ORDER BY rank LIMIT 5
    """).fetchall()
    print(f"{'Rank':<6} {'Product':<30} {'Category':<12} {'Revenue':<12}")
    for r in rows:
        print(f"#{r[0]:<5} {r[1][:29]:<30} {r[2]:<12} {r[3]:<12}")

    print("\n👥 Top 5 Customers by LTV:")
    print("-" * 50)
    rows = conn.execute("""
        SELECT full_name, total_orders, printf('$%.2f', lifetime_value)
        FROM mart_customer_ltv ORDER BY lifetime_value DESC LIMIT 5
    """).fetchall()
    print(f"{'Customer':<30} {'Orders':<10} {'LTV':<12}")
    for r in rows:
        print(f"{r[0][:29]:<30} {r[1]:<10} {r[2]:<12}")

    print("\n📅 Daily Revenue Summary:")
    print("-" * 50)
    rows = conn.execute("""
        SELECT order_date, total_orders, printf('$%.2f', total_revenue), printf('$%.2f', average_order_value)
        FROM mart_revenue_daily ORDER BY order_date DESC LIMIT 3
    """).fetchall()
    print(f"{'Date':<12} {'Orders':<10} {'Revenue':<15} {'Avg Order':<12}")
    for r in rows:
        print(f"{r[0]:<12} {r[1]:<10} {r[2]:<15} {r[3]:<12}")

    conn.close()

    print("\n" + "=" * 60)
    print("✅ PIPELINE COMPLETED SUCCESSFULLY!")
    print("=" * 60)
    print(f"\n📁 Database: {db_path}")
    print(f"📁 CSV files: {data_dir}")
    print("\nTo explore the database:")
    print(f"  sqlite3 {db_path}")
    print("  .tables")
    print("  SELECT * FROM mart_revenue_daily;")
    print()


if __name__ == "__main__":
    run_pipeline()
