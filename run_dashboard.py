"""
Simple Web Dashboard for Pipeline Results

A lightweight web server to display analytics results.
No external dependencies required - uses Python's built-in http.server.

Usage:
    python run_dashboard.py
    python run_dashboard.py --port 3000
    python run_dashboard.py --db chemin/vers/autre.db

Then open: http://localhost:8080
"""

import http.server
import socketserver
import sqlite3
import json
import argparse
from pathlib import Path
from urllib.parse import urlparse, parse_qs

# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                       🔧 CONFIGURATION DASHBOARD                             ║
# ╠══════════════════════════════════════════════════════════════════════════════╣
# ║  Modifiez ces paramètres selon vos besoins                                   ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

CONFIG = {
    # ─── Serveur ───────────────────────────────────────────────────────────────
    "port": 8080,                          # Port du serveur web
    "host": "localhost",                   # Adresse d'écoute (0.0.0.0 pour réseau)
    
    # ─── Base de données ───────────────────────────────────────────────────────
    "db_path": "demo_standalone.db",       # Chemin vers la base SQLite
    
    # ─── Affichage ─────────────────────────────────────────────────────────────
    "dashboard_title": "📊 E-Commerce Analytics Dashboard",
    "top_products_limit": 10,              # Nombre de top produits à afficher
    "top_customers_limit": 10,             # Nombre de top clients à afficher
    "revenue_days_limit": 30,              # Jours de revenus à afficher
    
    # ─── Format monétaire ──────────────────────────────────────────────────────
    "currency_symbol": "$",                # Symbole monétaire
    "currency_position": "before",         # "before" ($100) ou "after" (100€)
    "decimal_separator": ".",              # Séparateur décimal
    "thousands_separator": ",",            # Séparateur des milliers
    
    # ─── Couleurs (CSS) ────────────────────────────────────────────────────────
    "primary_color": "#667eea",            # Couleur principale
    "secondary_color": "#764ba2",          # Couleur secondaire (gradient)
    "success_color": "#48bb78",            # Couleur succès
    "warning_color": "#ed8936",            # Couleur avertissement
}

# Résolution du chemin DB (relatif au script ou absolu)
DB_PATH = Path(__file__).parent / CONFIG["db_path"]
PORT = CONFIG["port"]


def get_db_connection():
    """Get database connection."""
    return sqlite3.connect(str(DB_PATH))


def fetch_revenue_daily():
    """Fetch daily revenue data."""
    conn = get_db_connection()
    cursor = conn.execute(f"""
        SELECT order_date, total_orders, total_quantity, 
               total_revenue, average_order_value
        FROM mart_revenue_daily
        ORDER BY order_date DESC
        LIMIT {CONFIG['revenue_days_limit']}
    """)
    rows = cursor.fetchall()
    conn.close()
    return [
        {
            "date": r[0],
            "orders": r[1],
            "quantity": r[2],
            "revenue": r[3],
            "aov": r[4]
        }
        for r in rows
    ]


def fetch_top_products():
    """Fetch top products data."""
    conn = get_db_connection()
    cursor = conn.execute(f"""
        SELECT rank, product_name, category, total_quantity,
               total_revenue, order_count
        FROM mart_top_products
        ORDER BY rank
        LIMIT {CONFIG['top_products_limit']}
    """)
    rows = cursor.fetchall()
    conn.close()
    return [
        {
            "rank": r[0],
            "name": r[1],
            "category": r[2],
            "quantity": r[3],
            "revenue": r[4],
            "orders": r[5]
        }
        for r in rows
    ]


def fetch_top_customers():
    """Fetch top customers by LTV."""
    conn = get_db_connection()
    cursor = conn.execute(f"""
        SELECT customer_id, full_name, email, total_orders,
               lifetime_value, average_order_value
        FROM mart_customer_ltv
        ORDER BY lifetime_value DESC
        LIMIT {CONFIG['top_customers_limit']}
    """)
    rows = cursor.fetchall()
    conn.close()
    return [
        {
            "id": r[0],
            "name": r[1],
            "email": r[2],
            "orders": r[3],
            "ltv": r[4],
            "aov": r[5]
        }
        for r in rows
    ]


def fetch_summary():
    """Fetch summary statistics."""
    conn = get_db_connection()
    
    # Revenue summary
    rev = conn.execute("""
        SELECT SUM(total_revenue), SUM(total_orders), AVG(average_order_value)
        FROM mart_revenue_daily
    """).fetchone()
    
    # Customer count
    cust = conn.execute("SELECT COUNT(*), AVG(lifetime_value) FROM mart_customer_ltv").fetchone()
    
    # Product count
    prod = conn.execute("SELECT COUNT(*) FROM mart_top_products").fetchone()
    
    conn.close()
    
    return {
        "total_revenue": rev[0] or 0,
        "total_orders": rev[1] or 0,
        "avg_order_value": rev[2] or 0,
        "total_customers": cust[0] or 0,
        "avg_ltv": cust[1] or 0,
        "total_products": prod[0] or 0
    }


HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>E-Commerce Analytics Dashboard</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
            min-height: 100vh;
            color: #fff;
            padding: 20px;
        }
        .container {
            max-width: 1400px;
            margin: 0 auto;
        }
        header {
            text-align: center;
            padding: 30px 0;
            margin-bottom: 30px;
        }
        header h1 {
            font-size: 2.5rem;
            background: linear-gradient(90deg, #00d9ff, #00ff88);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            margin-bottom: 10px;
        }
        header p {
            color: #888;
            font-size: 1.1rem;
        }
        .badge {
            display: inline-block;
            background: #00ff88;
            color: #1a1a2e;
            padding: 5px 15px;
            border-radius: 20px;
            font-size: 0.85rem;
            font-weight: bold;
            margin-top: 10px;
        }
        .summary-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        .summary-card {
            background: rgba(255,255,255,0.05);
            border-radius: 15px;
            padding: 25px;
            text-align: center;
            border: 1px solid rgba(255,255,255,0.1);
            transition: transform 0.3s, box-shadow 0.3s;
        }
        .summary-card:hover {
            transform: translateY(-5px);
            box-shadow: 0 10px 30px rgba(0,217,255,0.2);
        }
        .summary-card .value {
            font-size: 2rem;
            font-weight: bold;
            color: #00d9ff;
            margin-bottom: 5px;
        }
        .summary-card .label {
            color: #888;
            font-size: 0.9rem;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        .dashboard-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(450px, 1fr));
            gap: 25px;
        }
        .card {
            background: rgba(255,255,255,0.05);
            border-radius: 15px;
            padding: 25px;
            border: 1px solid rgba(255,255,255,0.1);
        }
        .card h2 {
            font-size: 1.3rem;
            margin-bottom: 20px;
            display: flex;
            align-items: center;
            gap: 10px;
        }
        .card h2 .icon {
            font-size: 1.5rem;
        }
        table {
            width: 100%;
            border-collapse: collapse;
        }
        th, td {
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid rgba(255,255,255,0.1);
        }
        th {
            color: #888;
            font-weight: 500;
            text-transform: uppercase;
            font-size: 0.75rem;
            letter-spacing: 1px;
        }
        td {
            font-size: 0.95rem;
        }
        tr:hover {
            background: rgba(255,255,255,0.03);
        }
        .rank {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            width: 28px;
            height: 28px;
            border-radius: 50%;
            font-weight: bold;
            font-size: 0.85rem;
        }
        .rank-1 { background: gold; color: #1a1a2e; }
        .rank-2 { background: silver; color: #1a1a2e; }
        .rank-3 { background: #cd7f32; color: #1a1a2e; }
        .rank-other { background: rgba(255,255,255,0.1); }
        .money {
            color: #00ff88;
            font-weight: 500;
        }
        .category-badge {
            background: rgba(0,217,255,0.2);
            color: #00d9ff;
            padding: 3px 10px;
            border-radius: 10px;
            font-size: 0.8rem;
        }
        footer {
            text-align: center;
            margin-top: 40px;
            padding: 20px;
            color: #666;
        }
        footer a {
            color: #00d9ff;
            text-decoration: none;
        }
        .refresh-btn {
            background: linear-gradient(90deg, #00d9ff, #00ff88);
            border: none;
            padding: 10px 25px;
            border-radius: 25px;
            color: #1a1a2e;
            font-weight: bold;
            cursor: pointer;
            margin-top: 10px;
            transition: transform 0.2s;
        }
        .refresh-btn:hover {
            transform: scale(1.05);
        }
        @media (max-width: 768px) {
            .dashboard-grid {
                grid-template-columns: 1fr;
            }
            header h1 {
                font-size: 1.8rem;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>🚀 E-Commerce Analytics Dashboard</h1>
            <p>Airflow Data Pipeline - Real-time Analytics</p>
            <span class="badge">✓ Pipeline Completed</span>
        </header>

        <div class="summary-grid" id="summary">
            <!-- Filled by JS -->
        </div>

        <div class="dashboard-grid">
            <div class="card">
                <h2><span class="icon">🏆</span> Top Products by Revenue</h2>
                <table id="products-table">
                    <thead>
                        <tr>
                            <th>Rank</th>
                            <th>Product</th>
                            <th>Category</th>
                            <th>Revenue</th>
                        </tr>
                    </thead>
                    <tbody></tbody>
                </table>
            </div>

            <div class="card">
                <h2><span class="icon">👥</span> Top Customers by LTV</h2>
                <table id="customers-table">
                    <thead>
                        <tr>
                            <th>Customer</th>
                            <th>Orders</th>
                            <th>LTV</th>
                            <th>AOV</th>
                        </tr>
                    </thead>
                    <tbody></tbody>
                </table>
            </div>
        </div>

        <footer>
            <p>Built with <a href="#">Airflow E-Commerce Pipeline</a> | 
               Data refreshed from SQLite</p>
            <button class="refresh-btn" onclick="loadData()">🔄 Refresh Data</button>
        </footer>
    </div>

    <script>
        function formatMoney(value) {
            return '$' + parseFloat(value).toLocaleString('en-US', {
                minimumFractionDigits: 2,
                maximumFractionDigits: 2
            });
        }

        function getRankClass(rank) {
            if (rank === 1) return 'rank rank-1';
            if (rank === 2) return 'rank rank-2';
            if (rank === 3) return 'rank rank-3';
            return 'rank rank-other';
        }

        async function loadData() {
            try {
                // Load summary
                const summaryRes = await fetch('/api/summary');
                const summary = await summaryRes.json();
                
                document.getElementById('summary').innerHTML = `
                    <div class="summary-card">
                        <div class="value">${formatMoney(summary.total_revenue)}</div>
                        <div class="label">Total Revenue</div>
                    </div>
                    <div class="summary-card">
                        <div class="value">${summary.total_orders.toLocaleString()}</div>
                        <div class="label">Total Orders</div>
                    </div>
                    <div class="summary-card">
                        <div class="value">${formatMoney(summary.avg_order_value)}</div>
                        <div class="label">Avg Order Value</div>
                    </div>
                    <div class="summary-card">
                        <div class="value">${summary.total_customers}</div>
                        <div class="label">Active Customers</div>
                    </div>
                    <div class="summary-card">
                        <div class="value">${formatMoney(summary.avg_ltv)}</div>
                        <div class="label">Avg Customer LTV</div>
                    </div>
                    <div class="summary-card">
                        <div class="value">${summary.total_products}</div>
                        <div class="label">Products Sold</div>
                    </div>
                `;

                // Load top products
                const productsRes = await fetch('/api/products');
                const products = await productsRes.json();
                
                const productsBody = document.querySelector('#products-table tbody');
                productsBody.innerHTML = products.map(p => `
                    <tr>
                        <td><span class="${getRankClass(p.rank)}">${p.rank}</span></td>
                        <td>${p.name}</td>
                        <td><span class="category-badge">${p.category}</span></td>
                        <td class="money">${formatMoney(p.revenue)}</td>
                    </tr>
                `).join('');

                // Load top customers
                const customersRes = await fetch('/api/customers');
                const customers = await customersRes.json();
                
                const customersBody = document.querySelector('#customers-table tbody');
                customersBody.innerHTML = customers.map(c => `
                    <tr>
                        <td>${c.name}</td>
                        <td>${c.orders}</td>
                        <td class="money">${formatMoney(c.ltv)}</td>
                        <td class="money">${formatMoney(c.aov)}</td>
                    </tr>
                `).join('');

            } catch (error) {
                console.error('Error loading data:', error);
            }
        }

        // Load data on page load
        loadData();
    </script>
</body>
</html>
"""


class DashboardHandler(http.server.SimpleHTTPRequestHandler):
    """Custom HTTP handler for the dashboard."""

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/" or path == "/index.html":
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(HTML_TEMPLATE.encode())

        elif path == "/api/summary":
            self.send_json(fetch_summary())

        elif path == "/api/products":
            self.send_json(fetch_top_products())

        elif path == "/api/customers":
            self.send_json(fetch_top_customers())

        elif path == "/api/revenue":
            self.send_json(fetch_revenue_daily())

        else:
            self.send_error(404, "Not Found")

    def send_json(self, data):
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

    def log_message(self, format, *args):
        print(f"[{self.log_date_time_string()}] {args[0]}")


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="E-Commerce Analytics Dashboard")
    parser.add_argument("--port", type=int, default=CONFIG["port"],
                        help=f"Port du serveur (défaut: {CONFIG['port']})")
    parser.add_argument("--db", type=str, default=None,
                        help="Chemin vers la base SQLite")
    parser.add_argument("--host", type=str, default=CONFIG["host"],
                        help=f"Adresse d'écoute (défaut: {CONFIG['host']})")
    return parser.parse_args()


def main():
    """Start the dashboard server."""
    global DB_PATH, PORT
    
    # Parse command line arguments
    args = parse_args()
    PORT = args.port
    if args.db:
        DB_PATH = Path(args.db)
    
    # Check if database exists
    if not DB_PATH.exists():
        print("❌ Database not found!")
        print(f"   Expected: {DB_PATH}")
        print("\n   Run the pipeline first:")
        print("   python run_standalone_demo.py")
        return

    print("\n" + "=" * 50)
    print(f"🚀 {CONFIG['dashboard_title']}")
    print("=" * 50)
    print(f"\n   Database: {DB_PATH}")
    print(f"\n   Starting server on {args.host}:{PORT}...")
    print(f"\n   🌐 Open in browser: http://{args.host}:{PORT}")
    print("\n   Press Ctrl+C to stop\n")

    with socketserver.TCPServer((args.host, PORT), DashboardHandler) as httpd:
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\n\n👋 Server stopped.")


if __name__ == "__main__":
    main()
