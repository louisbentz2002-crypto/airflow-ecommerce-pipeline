"""
Local Demo Script - Run Pipeline Without Docker

This script demonstrates the data pipeline locally using SQLite
instead of PostgreSQL, without requiring Airflow or Docker.

Usage:
    python run_local_demo.py
"""

import os
import sys
import sqlite3
from datetime import date, datetime
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from data_generator import EcommerceDataGenerator
from validators import validate_csv_schema, EXPECTED_SCHEMAS


def create_database(db_path: str = "demo_ecommerce.db") -> sqlite3.Connection:
    """Create SQLite database with all schemas."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create raw tables
    cursor.executescript("""
        -- RAW TABLES
        CREATE TABLE IF NOT EXISTS raw_orders (
            order_id TEXT,
            customer_id TEXT,
            product_id TEXT,
            quantity INTEGER,
            unit_price REAL,
            total_amount REAL,
            order_date TEXT,
            order_status TEXT,
            shipping_address TEXT,
            _loaded_at TEXT,
            _source_file TEXT,
            _execution_date TEXT
        );

        CREATE TABLE IF NOT EXISTS raw_customers (
            customer_id TEXT,
            first_name TEXT,
            last_name TEXT,
            email TEXT,
            phone TEXT,
            address TEXT,
            city TEXT,
            country TEXT,
            created_at TEXT,
            _loaded_at TEXT,
            _source_file TEXT,
            _execution_date TEXT
        );

        CREATE TABLE IF NOT EXISTS raw_products (
            product_id TEXT,
            product_name TEXT,
            category TEXT,
            subcategory TEXT,
            price REAL,
            cost REAL,
            stock_quantity INTEGER,
            supplier TEXT,
            _loaded_at TEXT,
            _source_file TEXT,
            _execution_date TEXT
        );

        CREATE TABLE IF NOT EXISTS raw_payments (
            payment_id TEXT,
            order_id TEXT,
            payment_method TEXT,
            payment_amount REAL,
            payment_date TEXT,
            payment_status TEXT,
            _loaded_at TEXT,
            _source_file TEXT,
            _execution_date TEXT
        );

        -- STAGING TABLES
        CREATE TABLE IF NOT EXISTS stg_orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            product_id INTEGER,
            quantity INTEGER,
            unit_price REAL,
            total_amount REAL,
            order_date TEXT,
            order_status TEXT,
            shipping_address TEXT,
            _loaded_at TEXT,
            _execution_date TEXT
        );

        CREATE TABLE IF NOT EXISTS stg_customers (
            customer_id INTEGER PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            full_name TEXT,
            email TEXT,
            phone TEXT,
            address TEXT,
            city TEXT,
            country TEXT,
            created_at TEXT,
            _loaded_at TEXT,
            _execution_date TEXT
        );

        CREATE TABLE IF NOT EXISTS stg_products (
            product_id INTEGER PRIMARY KEY,
            product_name TEXT,
            category TEXT,
            subcategory TEXT,
            price REAL,
            cost REAL,
            margin REAL,
            margin_pct REAL,
            stock_quantity INTEGER,
            supplier TEXT,
            _loaded_at TEXT,
            _execution_date TEXT
        );

        CREATE TABLE IF NOT EXISTS stg_payments (
            payment_id INTEGER PRIMARY KEY,
            order_id INTEGER,
            payment_method TEXT,
            payment_amount REAL,
            payment_date TEXT,
            payment_status TEXT,
            _loaded_at TEXT,
            _execution_date TEXT
        );

        -- MART TABLES
        CREATE TABLE IF NOT EXISTS mart_revenue_daily (
            order_date TEXT PRIMARY KEY,
            total_orders INTEGER,
            total_quantity INTEGER,
            total_revenue REAL,
            average_order_value REAL,
            _updated_at TEXT
        );

        CREATE TABLE IF NOT EXISTS mart_top_products (
            product_id INTEGER,
            product_name TEXT,
            category TEXT,
            total_quantity INTEGER,
            total_revenue REAL,
            order_count INTEGER,
            rank INTEGER,
            period_start TEXT,
            period_end TEXT,
            _updated_at TEXT,
            PRIMARY KEY (product_id, period_start)
        );

        CREATE TABLE IF NOT EXISTS mart_customer_ltv (
            customer_id INTEGER PRIMARY KEY,
            full_name TEXT,
            email TEXT,
            first_order_date TEXT,
            last_order_date TEXT,
            total_orders INTEGER,
            total_quantity INTEGER,
            lifetime_value REAL,
            average_order_value REAL,
            customer_tenure_days INTEGER,
            _updated_at TEXT
        );
    """)

    conn.commit()
    return conn


def step_extract(output_dir: Path) -> dict:
    """Step 1: Extract - Generate sample data."""
    print("\n" + "=" * 60)
    print("STEP 1: EXTRACT - Generating sample data...")
    print("=" * 60)

    generator = EcommerceDataGenerator(seed=42)
    files = generator.generate_all(
        output_dir=output_dir,
        n_customers=100,
        n_products=50,
        n_orders=500,
        date=date.today(),
    )

    for entity, path in files.items():
        print(f"  ✓ Generated {entity}: {path}")

    return files


def step_validate(files: dict) -> bool:
    """Step 2: Validate - Check schema of generated files."""
    print("\n" + "=" * 60)
    print("STEP 2: VALIDATE - Checking data schemas...")
    print("=" * 60)

    all_valid = True
    for entity, path in files.items():
        is_valid, errors = validate_csv_schema(path, EXPECTED_SCHEMAS[entity])
        status = "✓ PASS" if is_valid else "✗ FAIL"
        print(f"  {status}: {entity}")
        if errors:
            for err in errors:
                print(f"      - {err}")
            all_valid = False

    return all_valid


def step_load_raw(conn: sqlite3.Connection, files: dict, execution_date: str):
    """Step 3: Load - Load data into raw tables."""
    import pandas as pd

    print("\n" + "=" * 60)
    print("STEP 3: LOAD RAW - Loading data into raw layer...")
    print("=" * 60)

    for entity, path in files.items():
        table_name = f"raw_{entity}"
        df = pd.read_csv(path)

        # Add metadata
        df["_loaded_at"] = datetime.utcnow().isoformat()
        df["_source_file"] = Path(path).name
        df["_execution_date"] = execution_date

        # Clear existing data for this date
        conn.execute(f"DELETE FROM {table_name} WHERE _execution_date = ?", (execution_date,))

        # Insert data
        df.to_sql(table_name, conn, if_exists="append", index=False)
        print(f"  ✓ Loaded {len(df)} rows into {table_name}")

    conn.commit()


def step_dq_raw(conn: sqlite3.Connection, execution_date: str) -> bool:
    """Step 4: Data Quality checks on raw layer."""
    print("\n" + "=" * 60)
    print("STEP 4: DQ RAW - Running data quality checks...")
    print("=" * 60)

    checks = [
        ("Count orders > 0", "SELECT COUNT(*) > 0 FROM raw_orders"),
        ("Count customers > 0", "SELECT COUNT(*) > 0 FROM raw_customers"),
        ("Count products > 0", "SELECT COUNT(*) > 0 FROM raw_products"),
        ("Count payments > 0", "SELECT COUNT(*) > 0 FROM raw_payments"),
        ("Orders have order_id", "SELECT COUNT(*) = 0 FROM raw_orders WHERE order_id IS NULL"),
    ]

    all_passed = True
    for name, sql in checks:
        result = conn.execute(sql).fetchone()[0]
        passed = bool(result)
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"  {status}: {name}")
        if not passed:
            all_passed = False

    return all_passed


def step_transform_staging(conn: sqlite3.Connection):
    """Step 5: Transform - Run staging transformations."""
    print("\n" + "=" * 60)
    print("STEP 5: TRANSFORM STAGING - Cleaning and typing data...")
    print("=" * 60)

    # Staging Orders
    conn.executescript("""
        DELETE FROM stg_orders;

        INSERT INTO stg_orders (
            order_id, customer_id, product_id, quantity, unit_price,
            total_amount, order_date, order_status, shipping_address,
            _loaded_at, _execution_date
        )
        SELECT
            CAST(order_id AS INTEGER),
            CAST(customer_id AS INTEGER),
            CAST(product_id AS INTEGER),
            COALESCE(quantity, 1),
            COALESCE(unit_price, 0),
            COALESCE(total_amount, quantity * unit_price),
            order_date,
            UPPER(TRIM(COALESCE(order_status, 'UNKNOWN'))),
            TRIM(shipping_address),
            _loaded_at,
            _execution_date
        FROM raw_orders
        WHERE order_id IS NOT NULL
        GROUP BY CAST(order_id AS INTEGER);
    """)
    count = conn.execute("SELECT COUNT(*) FROM stg_orders").fetchone()[0]
    print(f"  ✓ stg_orders: {count} rows")

    # Staging Customers
    conn.executescript("""
        DELETE FROM stg_customers;

        INSERT INTO stg_customers (
            customer_id, first_name, last_name, full_name, email,
            phone, address, city, country, created_at, _loaded_at, _execution_date
        )
        SELECT
            CAST(customer_id AS INTEGER),
            TRIM(first_name),
            TRIM(last_name),
            TRIM(first_name) || ' ' || TRIM(last_name),
            LOWER(TRIM(email)),
            phone,
            address,
            city,
            UPPER(TRIM(COALESCE(country, 'UNKNOWN'))),
            created_at,
            _loaded_at,
            _execution_date
        FROM raw_customers
        WHERE customer_id IS NOT NULL
        GROUP BY CAST(customer_id AS INTEGER);
    """)
    count = conn.execute("SELECT COUNT(*) FROM stg_customers").fetchone()[0]
    print(f"  ✓ stg_customers: {count} rows")

    # Staging Products
    conn.executescript("""
        DELETE FROM stg_products;

        INSERT INTO stg_products (
            product_id, product_name, category, subcategory, price, cost,
            margin, margin_pct, stock_quantity, supplier, _loaded_at, _execution_date
        )
        SELECT
            CAST(product_id AS INTEGER),
            TRIM(product_name),
            TRIM(category),
            TRIM(subcategory),
            COALESCE(price, 0),
            COALESCE(cost, 0),
            COALESCE(price, 0) - COALESCE(cost, 0),
            CASE WHEN price > 0 THEN ROUND((price - cost) / price * 100, 2) ELSE 0 END,
            COALESCE(stock_quantity, 0),
            supplier,
            _loaded_at,
            _execution_date
        FROM raw_products
        WHERE product_id IS NOT NULL
        GROUP BY CAST(product_id AS INTEGER);
    """)
    count = conn.execute("SELECT COUNT(*) FROM stg_products").fetchone()[0]
    print(f"  ✓ stg_products: {count} rows")

    # Staging Payments
    conn.executescript("""
        DELETE FROM stg_payments;

        INSERT INTO stg_payments (
            payment_id, order_id, payment_method, payment_amount,
            payment_date, payment_status, _loaded_at, _execution_date
        )
        SELECT
            CAST(payment_id AS INTEGER),
            CAST(order_id AS INTEGER),
            UPPER(TRIM(payment_method)),
            COALESCE(payment_amount, 0),
            payment_date,
            UPPER(TRIM(COALESCE(payment_status, 'PENDING'))),
            _loaded_at,
            _execution_date
        FROM raw_payments
        WHERE payment_id IS NOT NULL
        GROUP BY CAST(payment_id AS INTEGER);
    """)
    count = conn.execute("SELECT COUNT(*) FROM stg_payments").fetchone()[0]
    print(f"  ✓ stg_payments: {count} rows")

    conn.commit()


def step_transform_mart(conn: sqlite3.Connection):
    """Step 6: Transform - Build mart tables."""
    print("\n" + "=" * 60)
    print("STEP 6: TRANSFORM MART - Building analytics tables...")
    print("=" * 60)

    now = datetime.utcnow().isoformat()

    # Revenue Daily
    conn.execute("DELETE FROM mart_revenue_daily")
    conn.execute(f"""
        INSERT INTO mart_revenue_daily (
            order_date, total_orders, total_quantity, total_revenue,
            average_order_value, _updated_at
        )
        SELECT
            order_date,
            COUNT(DISTINCT order_id),
            SUM(quantity),
            SUM(total_amount),
            ROUND(SUM(total_amount) * 1.0 / COUNT(DISTINCT order_id), 2),
            '{now}'
        FROM stg_orders
        WHERE order_status IN ('COMPLETED', 'DELIVERED', 'SHIPPED', 'PAID')
        GROUP BY order_date
    """)
    count = conn.execute("SELECT COUNT(*) FROM mart_revenue_daily").fetchone()[0]
    print(f"  ✓ mart_revenue_daily: {count} rows")

    # Top Products
    conn.execute("DELETE FROM mart_top_products")
    conn.execute(f"""
        INSERT INTO mart_top_products (
            product_id, product_name, category, total_quantity,
            total_revenue, order_count, rank, period_start, period_end, _updated_at
        )
        SELECT
            o.product_id,
            p.product_name,
            p.category,
            SUM(o.quantity),
            SUM(o.total_amount),
            COUNT(DISTINCT o.order_id),
            ROW_NUMBER() OVER (ORDER BY SUM(o.total_amount) DESC),
            DATE('now', '-7 days'),
            DATE('now'),
            '{now}'
        FROM stg_orders o
        JOIN stg_products p ON o.product_id = p.product_id
        WHERE o.order_status IN ('COMPLETED', 'DELIVERED', 'SHIPPED', 'PAID')
        GROUP BY o.product_id, p.product_name, p.category
        ORDER BY SUM(o.total_amount) DESC
        LIMIT 50
    """)
    count = conn.execute("SELECT COUNT(*) FROM mart_top_products").fetchone()[0]
    print(f"  ✓ mart_top_products: {count} rows")

    # Customer LTV
    conn.execute("DELETE FROM mart_customer_ltv")
    conn.execute(f"""
        INSERT INTO mart_customer_ltv (
            customer_id, full_name, email, first_order_date, last_order_date,
            total_orders, total_quantity, lifetime_value, average_order_value,
            customer_tenure_days, _updated_at
        )
        SELECT
            o.customer_id,
            c.full_name,
            c.email,
            MIN(o.order_date),
            MAX(o.order_date),
            COUNT(DISTINCT o.order_id),
            SUM(o.quantity),
            SUM(o.total_amount),
            ROUND(SUM(o.total_amount) * 1.0 / COUNT(DISTINCT o.order_id), 2),
            CAST(JULIANDAY(MAX(o.order_date)) - JULIANDAY(MIN(o.order_date)) AS INTEGER),
            '{now}'
        FROM stg_orders o
        JOIN stg_customers c ON o.customer_id = c.customer_id
        WHERE o.order_status IN ('COMPLETED', 'DELIVERED', 'SHIPPED', 'PAID')
        GROUP BY o.customer_id, c.full_name, c.email
    """)
    count = conn.execute("SELECT COUNT(*) FROM mart_customer_ltv").fetchone()[0]
    print(f"  ✓ mart_customer_ltv: {count} rows")

    conn.commit()


def step_dq_mart(conn: sqlite3.Connection) -> bool:
    """Step 7: Data Quality checks on mart layer."""
    print("\n" + "=" * 60)
    print("STEP 7: DQ MART - Running final quality checks...")
    print("=" * 60)

    checks = [
        ("Revenue non-negative", "SELECT COUNT(*) = 0 FROM mart_revenue_daily WHERE total_revenue < 0"),
        ("No duplicate dates", "SELECT COUNT(*) = COUNT(DISTINCT order_date) FROM mart_revenue_daily"),
        ("LTV non-negative", "SELECT COUNT(*) = 0 FROM mart_customer_ltv WHERE lifetime_value < 0"),
        ("Top products exist", "SELECT COUNT(*) > 0 FROM mart_top_products"),
    ]

    all_passed = True
    for name, sql in checks:
        result = conn.execute(sql).fetchone()[0]
        passed = bool(result)
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"  {status}: {name}")
        if not passed:
            all_passed = False

    return all_passed


def show_results(conn: sqlite3.Connection):
    """Display sample results from mart tables."""
    print("\n" + "=" * 60)
    print("RESULTS - Sample data from analytics tables")
    print("=" * 60)

    # Revenue Summary
    print("\n📊 Daily Revenue Summary:")
    print("-" * 50)
    result = conn.execute("""
        SELECT
            order_date,
            total_orders,
            total_quantity,
            printf('%.2f', total_revenue) as revenue,
            printf('%.2f', average_order_value) as aov
        FROM mart_revenue_daily
        ORDER BY order_date DESC
        LIMIT 5
    """).fetchall()
    print(f"{'Date':<12} {'Orders':<8} {'Qty':<6} {'Revenue':<12} {'AOV':<10}")
    for row in result:
        print(f"{row[0]:<12} {row[1]:<8} {row[2]:<6} ${row[3]:<11} ${row[4]:<9}")

    # Top Products
    print("\n🏆 Top 5 Products by Revenue:")
    print("-" * 50)
    result = conn.execute("""
        SELECT
            rank,
            product_name,
            category,
            printf('%.2f', total_revenue) as revenue
        FROM mart_top_products
        ORDER BY rank
        LIMIT 5
    """).fetchall()
    print(f"{'Rank':<6} {'Product':<25} {'Category':<15} {'Revenue':<12}")
    for row in result:
        print(f"#{row[0]:<5} {row[1][:24]:<25} {row[2]:<15} ${row[3]:<11}")

    # Top Customers
    print("\n👥 Top 5 Customers by LTV:")
    print("-" * 50)
    result = conn.execute("""
        SELECT
            full_name,
            total_orders,
            printf('%.2f', lifetime_value) as ltv
        FROM mart_customer_ltv
        ORDER BY lifetime_value DESC
        LIMIT 5
    """).fetchall()
    print(f"{'Customer':<25} {'Orders':<10} {'LTV':<12}")
    for row in result:
        print(f"{row[0][:24]:<25} {row[1]:<10} ${row[2]:<11}")


def main():
    """Run the complete pipeline demo."""
    print("\n" + "=" * 60)
    print("🚀 AIRFLOW E-COMMERCE PIPELINE - LOCAL DEMO")
    print("=" * 60)
    print("Running pipeline without Docker using SQLite...")

    # Setup
    project_dir = Path(__file__).parent
    data_dir = project_dir / "data" / "incoming"
    db_path = project_dir / "demo_ecommerce.db"
    execution_date = date.today().isoformat()

    # Create database
    conn = create_database(str(db_path))
    print(f"\n✓ Database created: {db_path}")

    try:
        # Run pipeline steps
        files = step_extract(data_dir)
        step_validate(files)
        step_load_raw(conn, files, execution_date)
        step_dq_raw(conn, execution_date)
        step_transform_staging(conn)
        step_transform_mart(conn)
        step_dq_mart(conn)
        show_results(conn)

        print("\n" + "=" * 60)
        print("✅ PIPELINE COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print(f"\nDatabase saved to: {db_path}")
        print("You can explore it with any SQLite client.")
        print("\nTo view in terminal:")
        print(f"  sqlite3 {db_path}")
        print("  .tables")
        print("  SELECT * FROM mart_revenue_daily;")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
